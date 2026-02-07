"""Ignition Playbook Extractor — simulated trades from momentum score crossovers.

Reads enriched JSONL (momentum snapshots + price context) and produces:
1. Simulated trades from score-crossover logic (entry when score crosses above
   threshold, exit when it drops below exit threshold)
2. WIN vs LOSS entry/exit metric profiles
3. Threshold recommendations for IGNITION gate

Design note: The momentum engine state machine (BUILDING/PEAKED/DECAYING)
requires BUILDING->PEAKED->DECAYING transitions, but premarket gappers often
stay in BUILDING the entire session without hitting the PEAKED threshold (80+).
Score-crossover trades capture the real momentum oscillations.
"""

from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Premarket term mapping (report display only - engine states unchanged)
STATE_TO_PREMARKET = {
    "NEUTRAL": "CALM",
    "BUILDING": "BUILD",
    "PEAKED": "IGNITION",
    "DECAYING": "FADE",
    "REVERSING": "REVERSAL",
}

# Metrics we track for profiles
PROFILE_METRICS = [
    "momentum_score", "nofi", "l2_pressure", "velocity",
    "absorption", "spread_dynamics", "confidence",
]

# Score-crossover thresholds for simulated trades
ENTRY_SCORE_THRESHOLD = 60.0   # Enter when score crosses above this
EXIT_SCORE_THRESHOLD = 45.0    # Exit when score drops below this


@dataclass
class SimulatedTrade:
    """A simulated trade derived from momentum score crossovers."""
    symbol: str
    entry_time: str
    exit_time: str | None
    entry_price: float
    exit_price: float | None
    pnl_pct: float | None
    hold_seconds: float | None
    entry_state: str          # Engine state at entry
    exit_state: str | None    # Engine state at exit
    entry_score: float
    exit_score: float | None
    peak_score: float         # Highest score during trade
    entry_metrics: dict[str, float] = field(default_factory=dict)
    exit_metrics: dict[str, float] = field(default_factory=dict)
    is_win: bool | None = None    # None = incomplete
    is_complete: bool = True


@dataclass
class StateStats:
    """Statistics for one momentum state."""
    state: str
    premarket_term: str
    count: int
    total_seconds: float
    avg_seconds: float


@dataclass
class MetricDistribution:
    """Distribution stats for one metric."""
    metric: str
    mean: float
    median: float
    std: float
    p25: float
    p75: float
    min_val: float
    max_val: float
    count: int


@dataclass
class ThresholdRecommendation:
    """A recommended gate threshold for one metric."""
    parameter: str
    value: float
    basis: str
    confidence_level: str   # "HIGH", "MEDIUM", "LOW_CONFIDENCE"


@dataclass
class PlaybookResult:
    """Complete playbook analysis result."""
    symbol: str
    date: str
    window: str
    total_snapshots: int
    state_stats: list[StateStats]
    transition_matrix: dict[str, dict[str, int]]
    simulated_trades: list[SimulatedTrade]
    win_count: int
    loss_count: int
    incomplete_count: int
    win_rate: float | None
    win_entry_profiles: dict[str, MetricDistribution]
    loss_entry_profiles: dict[str, MetricDistribution]
    win_exit_profiles: dict[str, MetricDistribution]
    loss_exit_profiles: dict[str, MetricDistribution]
    thresholds: list[ThresholdRecommendation]
    data_quality: dict[str, float | str]

    def to_dict(self) -> dict:
        """Serialize to JSON-safe dict."""
        return {
            "symbol": self.symbol,
            "date": self.date,
            "window": self.window,
            "total_snapshots": self.total_snapshots,
            "state_stats": [
                {"state": s.state, "premarket_term": s.premarket_term,
                 "count": s.count, "total_seconds": round(s.total_seconds, 1),
                 "avg_seconds": round(s.avg_seconds, 1)}
                for s in self.state_stats
            ],
            "transition_matrix": self.transition_matrix,
            "simulated_trades": [
                {
                    "symbol": t.symbol,
                    "entry_time": t.entry_time, "exit_time": t.exit_time,
                    "entry_price": t.entry_price, "exit_price": t.exit_price,
                    "pnl_pct": round(t.pnl_pct, 4) if t.pnl_pct is not None else None,
                    "hold_seconds": round(t.hold_seconds, 1) if t.hold_seconds is not None else None,
                    "entry_state": t.entry_state, "exit_state": t.exit_state,
                    "entry_score": round(t.entry_score, 2),
                    "exit_score": round(t.exit_score, 2) if t.exit_score is not None else None,
                    "peak_score": round(t.peak_score, 2),
                    "is_win": t.is_win, "is_complete": t.is_complete,
                    "entry_metrics": {k: round(v, 4) for k, v in t.entry_metrics.items()},
                    "exit_metrics": {k: round(v, 4) for k, v in t.exit_metrics.items()},
                }
                for t in self.simulated_trades
            ],
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "incomplete_count": self.incomplete_count,
            "win_rate": round(self.win_rate, 4) if self.win_rate is not None else None,
            "thresholds": [
                {"parameter": t.parameter, "value": round(t.value, 4),
                 "basis": t.basis, "confidence_level": t.confidence_level}
                for t in self.thresholds
            ],
            "data_quality": self.data_quality,
        }


def _extract_metrics(snapshot: dict) -> dict[str, float]:
    """Extract the 7 profile metrics from a snapshot dict."""
    return {m: float(snapshot.get(m, 0.0)) for m in PROFILE_METRICS}


def _compute_distribution(values: list[float], metric_name: str) -> MetricDistribution:
    """Compute distribution statistics for a list of values."""
    if not values:
        return MetricDistribution(
            metric=metric_name, mean=0.0, median=0.0, std=0.0,
            p25=0.0, p75=0.0, min_val=0.0, max_val=0.0, count=0,
        )
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    p25_idx = max(0, int(n * 0.25))
    p75_idx = min(n - 1, int(n * 0.75))
    return MetricDistribution(
        metric=metric_name,
        mean=statistics.mean(values),
        median=statistics.median(values),
        std=statistics.stdev(values) if n >= 2 else 0.0,
        p25=sorted_vals[p25_idx],
        p75=sorted_vals[p75_idx],
        min_val=min(values),
        max_val=max(values),
        count=n,
    )


def _compute_profiles(trades: list[SimulatedTrade], use_entry: bool) -> dict[str, MetricDistribution]:
    """Compute metric distributions from a list of trades (entry or exit side)."""
    profiles: dict[str, MetricDistribution] = {}
    for metric in PROFILE_METRICS:
        if use_entry:
            values = [t.entry_metrics.get(metric, 0.0) for t in trades]
        else:
            values = [t.exit_metrics.get(metric, 0.0) for t in trades if t.is_complete]
        profiles[metric] = _compute_distribution(values, metric)
    return profiles


def _build_state_stats(snapshots: list[dict]) -> tuple[list[StateStats], dict]:
    """Compute state run statistics and transition matrix from snapshots."""
    state_runs: list[tuple[str, float]] = []
    transition_matrix: dict[str, dict[str, int]] = {}
    prev_state: str | None = None
    run_start_ts: str | None = None

    for snap in snapshots:
        state = snap["momentum_state"]
        ts = snap["timestamp"]

        if prev_state is None:
            prev_state = state
            run_start_ts = ts
            continue

        if state != prev_state:
            try:
                start_dt = datetime.fromisoformat(run_start_ts)
                end_dt = datetime.fromisoformat(ts)
                duration = (end_dt - start_dt).total_seconds()
            except (ValueError, TypeError):
                duration = 0.0
            state_runs.append((prev_state, duration))

            if prev_state not in transition_matrix:
                transition_matrix[prev_state] = {}
            transition_matrix[prev_state][state] = transition_matrix[prev_state].get(state, 0) + 1

            prev_state = state
            run_start_ts = ts

    # Close final run
    if prev_state and run_start_ts and snapshots:
        try:
            start_dt = datetime.fromisoformat(run_start_ts)
            end_dt = datetime.fromisoformat(snapshots[-1]["timestamp"])
            duration = (end_dt - start_dt).total_seconds()
        except (ValueError, TypeError):
            duration = 0.0
        state_runs.append((prev_state, duration))

    state_groups: dict[str, list[float]] = {}
    for state, dur in state_runs:
        state_groups.setdefault(state, []).append(dur)

    state_stats = []
    for state, durations in sorted(state_groups.items()):
        state_stats.append(StateStats(
            state=state,
            premarket_term=STATE_TO_PREMARKET.get(state, state),
            count=len(durations),
            total_seconds=sum(durations),
            avg_seconds=statistics.mean(durations) if durations else 0.0,
        ))

    return state_stats, transition_matrix


def _simulate_score_crossover_trades(
    snapshots: list[dict],
    symbol: str,
    entry_threshold: float = ENTRY_SCORE_THRESHOLD,
    exit_threshold: float = EXIT_SCORE_THRESHOLD,
) -> list[SimulatedTrade]:
    """Simulate trades using score-crossover logic.

    Entry: score crosses above entry_threshold from below.
    Exit: score drops below exit_threshold from above.
    """
    trades: list[SimulatedTrade] = []
    in_trade = False
    entry_snap: dict | None = None
    peak_score = 0.0

    for i in range(1, len(snapshots)):
        prev_score = snapshots[i - 1]["momentum_score"]
        curr_score = snapshots[i]["momentum_score"]
        snap = snapshots[i]
        price = snap.get("last_trade_price", 0.0)

        if not in_trade:
            # Entry: crosses above threshold
            if curr_score >= entry_threshold and prev_score < entry_threshold:
                in_trade = True
                entry_snap = snap
                peak_score = curr_score
        else:
            # Track peak
            if curr_score > peak_score:
                peak_score = curr_score

            # Exit: drops below threshold
            if curr_score <= exit_threshold and prev_score > exit_threshold:
                entry_price = entry_snap.get("last_trade_price", 0.0)
                exit_price = price
                try:
                    entry_dt = datetime.fromisoformat(entry_snap["timestamp"])
                    exit_dt = datetime.fromisoformat(snap["timestamp"])
                    hold_secs = (exit_dt - entry_dt).total_seconds()
                except (ValueError, TypeError):
                    hold_secs = 0.0

                pnl_pct = ((exit_price - entry_price) / entry_price) if entry_price > 0 else 0.0

                trades.append(SimulatedTrade(
                    symbol=symbol,
                    entry_time=entry_snap["timestamp"],
                    exit_time=snap["timestamp"],
                    entry_price=entry_price,
                    exit_price=exit_price,
                    pnl_pct=pnl_pct,
                    hold_seconds=hold_secs,
                    entry_state=entry_snap["momentum_state"],
                    exit_state=snap["momentum_state"],
                    entry_score=entry_snap["momentum_score"],
                    exit_score=curr_score,
                    peak_score=peak_score,
                    entry_metrics=_extract_metrics(entry_snap),
                    exit_metrics=_extract_metrics(snap),
                    is_win=pnl_pct > 0,
                    is_complete=True,
                ))
                in_trade = False
                entry_snap = None
                peak_score = 0.0

    # Handle incomplete trade
    if in_trade and entry_snap:
        last_snap = snapshots[-1]
        entry_price = entry_snap.get("last_trade_price", 0.0)
        exit_price = last_snap.get("last_trade_price", 0.0)
        try:
            entry_dt = datetime.fromisoformat(entry_snap["timestamp"])
            exit_dt = datetime.fromisoformat(last_snap["timestamp"])
            hold_secs = (exit_dt - entry_dt).total_seconds()
        except (ValueError, TypeError):
            hold_secs = 0.0

        pnl_pct = ((exit_price - entry_price) / entry_price) if entry_price > 0 else 0.0

        trades.append(SimulatedTrade(
            symbol=symbol,
            entry_time=entry_snap["timestamp"],
            exit_time=last_snap["timestamp"],
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_pct=pnl_pct,
            hold_seconds=hold_secs,
            entry_state=entry_snap["momentum_state"],
            exit_state=last_snap["momentum_state"],
            entry_score=entry_snap["momentum_score"],
            exit_score=last_snap["momentum_score"],
            peak_score=peak_score,
            entry_metrics=_extract_metrics(entry_snap),
            exit_metrics=_extract_metrics(last_snap),
            is_win=None,
            is_complete=False,
        ))

    return trades


def _compute_thresholds(
    wins: list[SimulatedTrade],
    losses: list[SimulatedTrade],
    win_entry: dict[str, MetricDistribution],
) -> list[ThresholdRecommendation]:
    """Derive threshold recommendations from WIN entry profiles."""
    enough_data = len(wins) >= 3 and len(losses) >= 3
    confidence_level = "HIGH" if enough_data else ("MEDIUM" if len(wins) >= 2 else "LOW_CONFIDENCE")

    thresholds: list[ThresholdRecommendation] = []
    if not wins:
        return thresholds

    thresholds.append(ThresholdRecommendation(
        parameter="min_momentum_score",
        value=win_entry["momentum_score"].p25,
        basis=f"WIN entry p25 (n={win_entry['momentum_score'].count})",
        confidence_level=confidence_level,
    ))
    thresholds.append(ThresholdRecommendation(
        parameter="min_nofi",
        value=win_entry["nofi"].p25,
        basis=f"WIN entry p25 (n={win_entry['nofi'].count})",
        confidence_level=confidence_level,
    ))
    thresholds.append(ThresholdRecommendation(
        parameter="min_l2_pressure",
        value=win_entry["l2_pressure"].p25,
        basis=f"WIN entry p25 (n={win_entry['l2_pressure'].count})",
        confidence_level=confidence_level,
    ))
    thresholds.append(ThresholdRecommendation(
        parameter="min_velocity",
        value=win_entry["velocity"].p25,
        basis=f"WIN entry p25 (n={win_entry['velocity'].count})",
        confidence_level=confidence_level,
    ))
    thresholds.append(ThresholdRecommendation(
        parameter="max_spread_dynamics",
        value=win_entry["spread_dynamics"].p75,
        basis=f"WIN entry p75 cap (n={win_entry['spread_dynamics'].count})",
        confidence_level=confidence_level,
    ))
    thresholds.append(ThresholdRecommendation(
        parameter="min_confidence",
        value=win_entry["confidence"].p25,
        basis=f"WIN entry p25 (n={win_entry['confidence'].count})",
        confidence_level=confidence_level,
    ))

    return thresholds


def _compute_data_quality(snapshots: list[dict], wins: list, losses: list) -> dict:
    """Compute data quality metrics."""
    if len(snapshots) >= 2:
        try:
            first_ts = datetime.fromisoformat(snapshots[0]["timestamp"])
            last_ts = datetime.fromisoformat(snapshots[-1]["timestamp"])
            duration_min = (last_ts - first_ts).total_seconds() / 60
            snapshots_per_min = len(snapshots) / duration_min if duration_min > 0 else 0.0
        except (ValueError, TypeError):
            duration_min = 0.0
            snapshots_per_min = 0.0
    else:
        duration_min = 0.0
        snapshots_per_min = 0.0

    confidences = [s.get("confidence", 0.0) for s in snapshots]
    avg_confidence = statistics.mean(confidences) if confidences else 0.0
    low_conf_pct = sum(1 for c in confidences if c < 0.5) / len(confidences) if confidences else 0.0

    data_quality: dict = {
        "total_snapshots": len(snapshots),
        "duration_minutes": round(duration_min, 1),
        "snapshots_per_min": round(snapshots_per_min, 2),
        "avg_confidence": round(avg_confidence, 3),
        "low_confidence_pct": round(low_conf_pct, 3),
        "has_price_context": sum(1 for s in snapshots if s.get("last_trade_price", 0) > 0),
        "has_book_mid": sum(1 for s in snapshots if s.get("book_mid", 0) > 0),
    }

    warnings: list[str] = []
    if snapshots_per_min < 1.0:
        warnings.append("Low snapshot rate (<1/min)")
    if low_conf_pct > 0.5:
        warnings.append(f"High low-confidence rate ({low_conf_pct:.0%})")
    enough = len(wins) >= 3 and len(losses) >= 3
    if not enough:
        warnings.append(f"Only {len(wins)} wins / {len(losses)} losses")
    data_quality["warnings"] = "; ".join(warnings) if warnings else "None"

    return data_quality


def extract_playbook(
    enriched_jsonl_path: str,
    symbol: str,
    date_str: str,
    window: str = "07:00-10:00 ET",
    entry_threshold: float = ENTRY_SCORE_THRESHOLD,
    exit_threshold: float = EXIT_SCORE_THRESHOLD,
) -> PlaybookResult:
    """Run the full playbook extraction on an enriched JSONL file.

    Uses score-crossover logic for simulated trades:
    - Entry: momentum_score crosses above entry_threshold
    - Exit: momentum_score drops below exit_threshold
    """
    path = Path(enriched_jsonl_path)
    if not path.exists():
        raise FileNotFoundError(f"Enriched JSONL not found: {path}")

    snapshots: list[dict] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                snapshots.append(json.loads(line))

    if not snapshots:
        raise ValueError(f"No snapshots in {path}")

    logger.info("Loaded %d snapshots from %s", len(snapshots), path)

    # State statistics
    state_stats, transition_matrix = _build_state_stats(snapshots)

    # Simulated trades (score-crossover)
    trades = _simulate_score_crossover_trades(
        snapshots, symbol, entry_threshold, exit_threshold,
    )

    # WIN/LOSS split
    wins = [t for t in trades if t.is_win is True]
    losses = [t for t in trades if t.is_win is False]
    incomplete = [t for t in trades if t.is_win is None]

    win_entry = _compute_profiles(wins, use_entry=True)
    loss_entry = _compute_profiles(losses, use_entry=True)
    win_exit = _compute_profiles(wins, use_entry=False)
    loss_exit = _compute_profiles(losses, use_entry=False)

    complete_count = len(wins) + len(losses)
    win_rate = len(wins) / complete_count if complete_count > 0 else None

    thresholds = _compute_thresholds(wins, losses, win_entry)
    data_quality = _compute_data_quality(snapshots, wins, losses)

    result = PlaybookResult(
        symbol=symbol,
        date=date_str,
        window=window,
        total_snapshots=len(snapshots),
        state_stats=state_stats,
        transition_matrix=transition_matrix,
        simulated_trades=trades,
        win_count=len(wins),
        loss_count=len(losses),
        incomplete_count=len(incomplete),
        win_rate=win_rate,
        win_entry_profiles=win_entry,
        loss_entry_profiles=loss_entry,
        win_exit_profiles=win_exit,
        loss_exit_profiles=loss_exit,
        thresholds=thresholds,
        data_quality=data_quality,
    )

    logger.info(
        "Playbook: %s %s — %d trades (%d W / %d L / %d I) | thresholds=%d",
        symbol, date_str, len(trades), len(wins), len(losses), len(incomplete), len(thresholds),
    )

    return result


# ──────────────────────────────────────────────────────────────────────
# Cross-session aggregation
# ──────────────────────────────────────────────────────────────────────

@dataclass
class AggregatedPlaybook:
    """Aggregated analysis across multiple symbol/date sessions."""
    symbols: list[str]
    sessions: list[PlaybookResult]
    all_trades: list[SimulatedTrade]
    win_count: int
    loss_count: int
    incomplete_count: int
    win_rate: float | None
    avg_pnl_pct: float | None
    avg_win_pnl: float | None
    avg_loss_pnl: float | None
    expectancy: float | None       # avg_win * win_rate - avg_loss * loss_rate
    win_entry_profiles: dict[str, MetricDistribution]
    loss_entry_profiles: dict[str, MetricDistribution]
    win_exit_profiles: dict[str, MetricDistribution]
    loss_exit_profiles: dict[str, MetricDistribution]
    thresholds: list[ThresholdRecommendation]
    per_symbol_stats: list[dict]
    data_quality: dict

    def to_dict(self) -> dict:
        return {
            "symbols": self.symbols,
            "total_sessions": len(self.sessions),
            "total_trades": len(self.all_trades),
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "incomplete_count": self.incomplete_count,
            "win_rate": round(self.win_rate, 4) if self.win_rate is not None else None,
            "avg_pnl_pct": round(self.avg_pnl_pct, 4) if self.avg_pnl_pct is not None else None,
            "avg_win_pnl": round(self.avg_win_pnl, 4) if self.avg_win_pnl is not None else None,
            "avg_loss_pnl": round(self.avg_loss_pnl, 4) if self.avg_loss_pnl is not None else None,
            "expectancy": round(self.expectancy, 4) if self.expectancy is not None else None,
            "thresholds": [
                {"parameter": t.parameter, "value": round(t.value, 4),
                 "basis": t.basis, "confidence_level": t.confidence_level}
                for t in self.thresholds
            ],
            "per_symbol_stats": self.per_symbol_stats,
            "data_quality": self.data_quality,
            "trades": [
                {
                    "symbol": t.symbol,
                    "entry_time": t.entry_time, "exit_time": t.exit_time,
                    "entry_price": round(t.entry_price, 4),
                    "exit_price": round(t.exit_price, 4) if t.exit_price else None,
                    "pnl_pct": round(t.pnl_pct, 4) if t.pnl_pct is not None else None,
                    "hold_seconds": round(t.hold_seconds, 1) if t.hold_seconds else None,
                    "entry_score": round(t.entry_score, 2),
                    "exit_score": round(t.exit_score, 2) if t.exit_score is not None else None,
                    "peak_score": round(t.peak_score, 2),
                    "is_win": t.is_win, "is_complete": t.is_complete,
                }
                for t in self.all_trades
            ],
        }


def aggregate_playbooks(sessions: list[PlaybookResult]) -> AggregatedPlaybook:
    """Aggregate multiple PlaybookResults into a unified cross-session analysis."""

    all_trades: list[SimulatedTrade] = []
    for s in sessions:
        all_trades.extend(s.simulated_trades)

    wins = [t for t in all_trades if t.is_win is True]
    losses = [t for t in all_trades if t.is_win is False]
    incomplete = [t for t in all_trades if t.is_win is None]
    complete = wins + losses

    complete_count = len(complete)
    win_rate = len(wins) / complete_count if complete_count > 0 else None

    # P&L stats
    complete_pnls = [t.pnl_pct for t in complete if t.pnl_pct is not None]
    win_pnls = [t.pnl_pct for t in wins if t.pnl_pct is not None]
    loss_pnls = [t.pnl_pct for t in losses if t.pnl_pct is not None]

    avg_pnl = statistics.mean(complete_pnls) if complete_pnls else None
    avg_win = statistics.mean(win_pnls) if win_pnls else None
    avg_loss = statistics.mean(loss_pnls) if loss_pnls else None

    # Expectancy = avg_win * win_rate - |avg_loss| * loss_rate
    expectancy = None
    if win_rate is not None and avg_win is not None and avg_loss is not None and complete_count > 0:
        loss_rate = len(losses) / complete_count
        expectancy = avg_win * win_rate - abs(avg_loss) * loss_rate

    # Merged profiles
    win_entry = _compute_profiles(wins, use_entry=True)
    loss_entry = _compute_profiles(losses, use_entry=True)
    win_exit = _compute_profiles(wins, use_entry=False)
    loss_exit = _compute_profiles(losses, use_entry=False)

    # Thresholds from aggregated data
    thresholds = _compute_thresholds(wins, losses, win_entry)

    # Per-symbol stats
    per_symbol: list[dict] = []
    for s in sessions:
        s_wins = [t for t in s.simulated_trades if t.is_win is True]
        s_losses = [t for t in s.simulated_trades if t.is_win is False]
        s_total = len(s_wins) + len(s_losses)
        s_pnls = [t.pnl_pct for t in s.simulated_trades if t.pnl_pct is not None and t.is_complete]
        per_symbol.append({
            "symbol": s.symbol,
            "date": s.date,
            "total_snapshots": s.total_snapshots,
            "trades": len(s.simulated_trades),
            "wins": len(s_wins),
            "losses": len(s_losses),
            "win_rate": round(len(s_wins) / s_total, 2) if s_total > 0 else None,
            "avg_pnl": round(statistics.mean(s_pnls), 4) if s_pnls else None,
            "total_pnl": round(sum(s_pnls), 4) if s_pnls else None,
        })

    # Aggregated data quality
    total_snaps = sum(s.total_snapshots for s in sessions)
    data_quality = {
        "total_sessions": len(sessions),
        "total_snapshots": total_snaps,
        "total_completed_trades": complete_count,
        "total_incomplete_trades": len(incomplete),
        "symbols": [s.symbol for s in sessions],
        "dates": [s.date for s in sessions],
        "entry_threshold": ENTRY_SCORE_THRESHOLD,
        "exit_threshold": EXIT_SCORE_THRESHOLD,
    }

    return AggregatedPlaybook(
        symbols=[s.symbol for s in sessions],
        sessions=sessions,
        all_trades=all_trades,
        win_count=len(wins),
        loss_count=len(losses),
        incomplete_count=len(incomplete),
        win_rate=win_rate,
        avg_pnl_pct=avg_pnl,
        avg_win_pnl=avg_win,
        avg_loss_pnl=avg_loss,
        expectancy=expectancy,
        win_entry_profiles=win_entry,
        loss_entry_profiles=loss_entry,
        win_exit_profiles=win_exit,
        loss_exit_profiles=loss_exit,
        thresholds=thresholds,
        per_symbol_stats=per_symbol,
        data_quality=data_quality,
    )
