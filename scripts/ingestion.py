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

import time
from dataclasses import dataclass, field, replace
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

# ── Pipeline metrics (inspired by Claude Code cost-tracker) ──
# Lightweight, per-evaluate stats so callers can observe the pipeline
# without coupling to a logging framework.


@dataclass
class PipelineMetrics:
    """Snapshot of a single .evaluate() execution."""
    source_count: int = 0          # records returned by thunk
    result_count: int = 0          # records after all ops
    ops_applied: int = 0           # number of operator stages
    elapsed_ms: float = 0.0        # wall-clock time for evaluate()
    compacted: bool = False        # True if auto-budget truncated

    @property
    def drop_rate(self) -> float:
        """Fraction of source records filtered out."""
        return 1 - (self.result_count / self.source_count) if self.source_count else 0.0


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

    __slots__ = ("_thunk", "_ops", "_budget", "_last_metrics")

    def __init__(
        self,
        thunk: Callable[[], list[T]],
        ops: tuple[Operator, ...] = (),
        budget: Optional[int] = None,
    ):
        self._thunk = thunk
        self._ops = ops
        self._budget: Optional[int] = budget
        self._last_metrics: Optional[PipelineMetrics] = None

    # ── Builders (each returns a NEW RecordSet) ───────

    def _extend(self, op: Operator) -> RecordSet[T]:
        """Internal: return a new RecordSet with one more operator, preserving budget."""
        return RecordSet(self._thunk, self._ops + (op,), budget=self._budget)

    def filter(self, predicate: Callable[[T], bool]) -> RecordSet[T]:
        """Append a filter predicate — evaluated at runtime."""
        return self._extend(lambda rs, p=predicate: [r for r in rs if p(r)])

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
        return self._extend(lambda rs, k=key, rev=reverse: sorted(rs, key=k, reverse=rev))

    def map(self, transform: Callable[[T], T]) -> RecordSet[T]:
        """Apply a per-record transformation at runtime."""
        return self._extend(lambda rs, t=transform: [t(r) for r in rs])

    def take(self, n: int) -> RecordSet[T]:
        """Limit to first n records (applied after preceding ops)."""
        return self._extend(lambda rs, limit=n: rs[:limit])

    def drop(self, n: int) -> RecordSet[T]:
        """Skip first n records."""
        return self._extend(lambda rs, skip=n: rs[skip:])

    def compact(self, budget: int) -> RecordSet[T]:
        """Set a record budget — auto-truncate after thunk if source exceeds it.

        Inspired by Claude Code's context compaction: when the raw source
        returns more records than `budget`, only the first `budget` are kept
        before operators run.  Prevents large intermediate sets from blowing
        up downstream sorts / joins.
        """
        return RecordSet(self._thunk, self._ops, budget=budget)

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
        return self._extend(lambda rs, f=fn: [item for r in rs for item in f(r)])

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
        """Materialize: run the thunk, apply compaction + all chained operators.

        Records PipelineMetrics accessible via `.metrics` after evaluation.
        """
        t0 = time.monotonic()
        raw = self._thunk()
        source_count = len(raw)
        compacted = False

        # Context compaction: if budget set and source exceeds it, truncate
        if self._budget is not None and len(raw) > self._budget:
            raw = raw[:self._budget]
            compacted = True

        result = _chain(*self._ops)(raw) if self._ops else raw

        self._last_metrics = PipelineMetrics(
            source_count=source_count,
            result_count=len(result),
            ops_applied=len(self._ops),
            elapsed_ms=(time.monotonic() - t0) * 1000,
            compacted=compacted,
        )
        return result

    @property
    def metrics(self) -> Optional[PipelineMetrics]:
        """Metrics from the most recent .evaluate() call, or None."""
        return self._last_metrics

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
        budget = f", budget={self._budget}" if self._budget else ""
        return f"RecordSet(ops={len(self._ops)}{budget})"


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
