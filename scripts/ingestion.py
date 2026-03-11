"""Functional ingestion pipeline — lazy evaluation over OptionsRecords.

All filter / sort / join / map operations build up a chain of lambdas.
Nothing touches Redis or transforms data until `.evaluate()` is called.

Usage:
    from ingestion import RecordSet, source

    accessor = RedisOptionsAccessor.from_env()

    # Build a lazy pipeline — nothing runs yet
    calls = (
        source.from_underlying(accessor, "AAPL")
        .filter(lambda r: r.option_type == "C")
        .filter(lambda r: (r.greeks.delta or 0) > 0.3)
        .sort(lambda r: r.pricing.bid or 0, reverse=True)
    )

    # Join two lazy sets at runtime — merges on symbol key
    positions = source.from_underlying(accessor, "AAPL")
    quotes    = source.from_underlying(accessor, "AAPL")
    merged = positions.join(quotes, key=lambda r: r.symbol)

    # Only NOW does it hit Redis and apply the chain
    results = calls.evaluate()

    # Shorthand: iterate directly (auto-evaluates)
    for record in calls:
        print(record.symbol, record.pricing.bid)
"""

from __future__ import annotations

from dataclasses import replace
from functools import reduce
from typing import (
    Callable,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    TypeVar,
)

from options_accessor import (
    Greeks,
    OptionsRecord,
    PnL,
    Pricing,
    RedisOptionsAccessor,
    Sizing,
)

T = TypeVar("T")

# ── Operator types ────────────────────────────────────
# Each operator is a lambda that transforms a list → list.
# They compose: op3(op2(op1(initial_records)))

Operator = Callable[[list[T]], list[T]]


def _identity(records: list[T]) -> list[T]:
    """Pass-through — the zero element of operator composition."""
    return records


def _chain(*ops: Operator) -> Operator:
    """Compose operators left-to-right: chain(a, b, c)(xs) == c(b(a(xs)))."""
    return lambda xs: reduce(lambda acc, op: op(acc), ops, xs)


# ── RecordSet — lazy, composable query over records ───

class RecordSet(Generic[T]):
    """A lazy set of records with deferred filter / sort / join / map.

    Nothing is evaluated until `.evaluate()` or iteration.  Every
    transformation returns a *new* RecordSet (immutable chain).

    Internal state:
        _thunk   — a zero-arg lambda that produces the raw list[T]
        _ops     — tuple of Operator lambdas applied after the thunk
    """

    __slots__ = ("_thunk", "_ops")

    def __init__(
        self,
        thunk: Callable[[], list[T]],
        ops: tuple[Operator, ...] = (),
    ):
        self._thunk = thunk
        self._ops = ops

    # ── Builders (each returns a NEW RecordSet) ───────

    def filter(self, predicate: Callable[[T], bool]) -> RecordSet[T]:
        """Append a filter predicate — evaluated at runtime."""
        op: Operator = lambda rs, p=predicate: [r for r in rs if p(r)]
        return RecordSet(self._thunk, self._ops + (op,))

    def exclude(self, predicate: Callable[[T], bool]) -> RecordSet[T]:
        """Inverse filter — exclude records matching predicate."""
        return self.filter(lambda r, p=predicate: not p(r))

    def sort(
        self,
        key: Callable[[T], object],
        *,
        reverse: bool = False,
    ) -> RecordSet[T]:
        """Append a sort — evaluated at runtime."""
        op: Operator = lambda rs, k=key, rev=reverse: sorted(rs, key=k, reverse=rev)
        return RecordSet(self._thunk, self._ops + (op,))

    def map(self, transform: Callable[[T], T]) -> RecordSet[T]:
        """Apply a per-record transformation at runtime."""
        op: Operator = lambda rs, t=transform: [t(r) for r in rs]
        return RecordSet(self._thunk, self._ops + (op,))

    def take(self, n: int) -> RecordSet[T]:
        """Limit to first n records (applied after preceding ops)."""
        op: Operator = lambda rs, limit=n: rs[:limit]
        return RecordSet(self._thunk, self._ops + (op,))

    def drop(self, n: int) -> RecordSet[T]:
        """Skip first n records."""
        op: Operator = lambda rs, skip=n: rs[skip:]
        return RecordSet(self._thunk, self._ops + (op,))

    def join(
        self,
        other: RecordSet[T],
        *,
        key: Callable[[T], str],
        merge: Optional[Callable[[T, T], T]] = None,
    ) -> RecordSet[T]:
        """Lazy inner join with another RecordSet on a key function.

        `merge` controls how two matching records combine. Default for
        OptionsRecord: overlay non-empty fields from `right` onto `left`.
        For other types, `right` wins.
        """
        merger = merge or _default_merge

        def joined_thunk(
            left_thunk=self._thunk,
            left_ops=self._ops,
            right_thunk=other._thunk,
            right_ops=other._ops,
            k=key,
            m=merger,
        ):
            left = _chain(*left_ops)(left_thunk()) if left_ops else left_thunk()
            right = _chain(*right_ops)(right_thunk()) if right_ops else right_thunk()
            right_by_key = {k(r): r for r in right}
            return [m(l, right_by_key[k(l)]) for l in left if k(l) in right_by_key]

        return RecordSet(joined_thunk)

    def left_join(
        self,
        other: RecordSet[T],
        *,
        key: Callable[[T], str],
        merge: Optional[Callable[[T, T], T]] = None,
    ) -> RecordSet[T]:
        """Lazy left join — keeps all left records, merges where right exists."""
        merger = merge or _default_merge

        def joined_thunk(
            left_thunk=self._thunk,
            left_ops=self._ops,
            right_thunk=other._thunk,
            right_ops=other._ops,
            k=key,
            m=merger,
        ):
            left = _chain(*left_ops)(left_thunk()) if left_ops else left_thunk()
            right = _chain(*right_ops)(right_thunk()) if right_ops else right_thunk()
            right_by_key = {k(r): r for r in right}
            return [m(l, right_by_key[k(l)]) if k(l) in right_by_key else l for l in left]

        return RecordSet(joined_thunk)

    def flat_map(self, fn: Callable[[T], Iterable[T]]) -> RecordSet[T]:
        """Map each record to zero-or-more records, then flatten."""
        op: Operator = lambda rs, f=fn: [item for r in rs for item in f(r)]
        return RecordSet(self._thunk, self._ops + (op,))

    def group_by(self, key: Callable[[T], str]) -> Callable[[], dict[str, list[T]]]:
        """Return a thunk that evaluates and groups records by key.

        Returns a callable (not a RecordSet) since the result type changes.
        """
        def grouped(k=key):
            groups: dict[str, list[T]] = {}
            for r in self.evaluate():
                groups.setdefault(k(r), []).append(r)
            return groups
        return grouped

    def reduce(self, fn: Callable[[T, T], T]) -> Callable[[], Optional[T]]:
        """Return a thunk that reduces evaluated records to a single value."""
        def reduced():
            records = self.evaluate()
            return reduce(fn, records) if records else None
        return reduced

    # ── Terminal operations ───────────────────────────

    def evaluate(self) -> list[T]:
        """Materialize: run the thunk, then apply all chained operators."""
        raw = self._thunk()
        return _chain(*self._ops)(raw) if self._ops else raw

    def first(self) -> Optional[T]:
        """Evaluate and return the first record, or None."""
        results = self.evaluate()
        return results[0] if results else None

    def count(self) -> int:
        """Evaluate and return the count."""
        return len(self.evaluate())

    def exists(self, predicate: Callable[[T], bool]) -> bool:
        """Evaluate and check if any record matches."""
        return any(predicate(r) for r in self.evaluate())

    def __iter__(self) -> Iterator[T]:
        return iter(self.evaluate())

    def __len__(self) -> int:
        return self.count()

    def __bool__(self) -> bool:
        return self.count() > 0

    def __repr__(self) -> str:
        return f"RecordSet(ops={len(self._ops)})"


# ── Default merge for OptionsRecord ───────────────────

def _overlay_sub(left, right):
    """Overlay non-None fields from right onto left for sub-attribute dataclasses."""
    updates = {
        k: getattr(right, k)
        for k in right.__dataclass_fields__
        if getattr(right, k) is not None
    }
    return replace(left, **updates) if updates else left


def _default_merge(left: OptionsRecord, right: OptionsRecord) -> OptionsRecord:
    """Merge two OptionsRecords — right's non-empty fields win."""
    return replace(
        left,
        pricing=_overlay_sub(left.pricing, right.pricing),
        greeks=_overlay_sub(left.greeks, right.greeks),
        sizing=_overlay_sub(left.sizing, right.sizing),
        pnl=_overlay_sub(left.pnl, right.pnl),
        side=right.side or left.side,
        order_type=right.order_type or left.order_type,
        status=right.status or left.status,
        order_id=right.order_id or left.order_id,
        orders=right.orders or left.orders,
        bars=right.bars or left.bars,
        updated_at=right.updated_at or left.updated_at,
    )


# ── Source constructors ───────────────────────────────
# Each returns a RecordSet whose thunk captures the accessor
# and only calls Redis when evaluated.


class source:
    """Namespace for RecordSet source factories."""

    @staticmethod
    def from_underlying(
        accessor: RedisOptionsAccessor,
        underlying: str,
    ) -> RecordSet[OptionsRecord]:
        """Lazy load all contracts for an underlying."""
        return RecordSet(lambda u=underlying: accessor.get_by_underlying(u))

    @staticmethod
    def from_symbols(
        accessor: RedisOptionsAccessor,
        symbols: Sequence[str],
    ) -> RecordSet[OptionsRecord]:
        """Lazy load specific contracts by OCC symbol."""
        def fetch(syms=symbols):
            return [r for s in syms if (r := accessor.get(s)) is not None]
        return RecordSet(fetch)

    @staticmethod
    def from_all(
        accessor: RedisOptionsAccessor,
    ) -> RecordSet[OptionsRecord]:
        """Lazy load every known contract."""
        def fetch():
            return [
                r
                for s in accessor.list_symbols()
                if (r := accessor.get(s)) is not None
            ]
        return RecordSet(fetch)

    @staticmethod
    def of(records: list[OptionsRecord]) -> RecordSet[OptionsRecord]:
        """Wrap an already-materialised list (still composable)."""
        return RecordSet(lambda rs=records: list(rs))

    @staticmethod
    def empty() -> RecordSet[OptionsRecord]:
        """An empty RecordSet."""
        return RecordSet(lambda: [])


# ── Predicate combinators ─────────────────────────────
# Helpers for building filter lambdas.

def is_call(r: OptionsRecord) -> bool:
    return r.option_type == "C"


def is_put(r: OptionsRecord) -> bool:
    return r.option_type == "P"


def has_greeks(r: OptionsRecord) -> bool:
    g = r.greeks
    return g.delta is not None or g.gamma is not None


def delta_between(lo: float, hi: float) -> Callable[[OptionsRecord], bool]:
    """Predicate factory: delta in [lo, hi]."""
    return lambda r, l=lo, h=hi: l <= (r.greeks.delta or 0) <= h


def strike_between(lo: float, hi: float) -> Callable[[OptionsRecord], bool]:
    return lambda r, l=lo, h=hi: l <= r.strike <= h


def expiration_eq(exp: str) -> Callable[[OptionsRecord], bool]:
    return lambda r, e=exp: r.expiration == e


def min_volume(threshold: int) -> Callable[[OptionsRecord], bool]:
    return lambda r, t=threshold: (r.sizing.volume or 0) >= t


def min_open_interest(threshold: int) -> Callable[[OptionsRecord], bool]:
    return lambda r, t=threshold: (r.sizing.open_interest or 0) >= t


def has_position(r: OptionsRecord) -> bool:
    return (r.sizing.qty or 0) != 0


# ── Sort key factories ────────────────────────────────

def by_strike(r: OptionsRecord) -> float:
    return r.strike


def by_expiration(r: OptionsRecord) -> str:
    return r.expiration


def by_delta(r: OptionsRecord) -> float:
    return abs(r.greeks.delta or 0)


def by_volume(r: OptionsRecord) -> int:
    return r.sizing.volume or 0


def by_bid(r: OptionsRecord) -> float:
    return r.pricing.bid or 0.0


def by_spread(r: OptionsRecord) -> float:
    return r.pricing.spread or float("inf")


# ── Functional sink (write pipeline) ──────────────────

def sink_quotes(
    accessor: RedisOptionsAccessor,
    ttl: int = 604800,
) -> Callable[[RecordSet[OptionsRecord]], int]:
    """Return a function that evaluates a RecordSet and writes each as a quote.

    Usage:
        write = sink_quotes(accessor)
        count = write(pipeline)   # evaluates + writes
    """
    def write(rs: RecordSet[OptionsRecord], a=accessor, t=ttl) -> int:
        records = rs.evaluate()
        for r in records:
            a.put_quote(r, ttl=t)
        return len(records)
    return write


def sink_positions(
    accessor: RedisOptionsAccessor,
    ttl: int = 2592000,
) -> Callable[[RecordSet[OptionsRecord]], int]:
    """Evaluate and write each record as a position."""
    def write(rs: RecordSet[OptionsRecord], a=accessor, t=ttl) -> int:
        records = rs.evaluate()
        for r in records:
            a.put_position(r, ttl=t)
        return len(records)
    return write


def sink_full(
    accessor: RedisOptionsAccessor,
    ttl: int = 2592000,
) -> Callable[[RecordSet[OptionsRecord]], int]:
    """Evaluate and write each record as a full record."""
    def write(rs: RecordSet[OptionsRecord], a=accessor, t=ttl) -> int:
        records = rs.evaluate()
        for r in records:
            a.put(r, ttl=t)
        return len(records)
    return write
