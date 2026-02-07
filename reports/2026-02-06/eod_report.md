# Morpheus EOD Report - Friday, February 6, 2026

## Day Summary

| Metric | Value |
|--------|-------|
| **Total Trades** | 8 |
| **Net P&L** | **+$60.53** |
| **Win Rate** | 75% (6W / 2L) |
| **Largest Win** | +$64.60 (BATL premarket +130.2%) |
| **Largest Loss** | -$4.78 (LUMN -11.1%) |
| **Symbols Traded** | BATL, LUMN, BOXL |
| **Symbols Monitored** | 21 |
| **Validation Status** | PASSED (0 violations) |

---

## Phase Breakdown

### Premarket (07:00 - 09:30 ET)
**Trades: 3/3 cap | P&L: +$65.23 | Win Rate: 100%**

| # | Symbol | Strategy | Entry | Exit | Shares | P&L | % | Exit Reason | Hold |
|---|--------|----------|-------|------|--------|-----|---|-------------|------|
| 1 | BATL | premarket_breakout | $1.24 | $2.855 | 40 | +$64.60 | +130.2% | TRAIL_STOP | 1m50s |
| 2 | BATL | catalyst_momentum | $2.855 | $2.865 | 11 | +$0.11 | +0.4% | TIME_STOP | 5m11s |
| 3 | BATL | catalyst_momentum | $2.865 | $2.900 | 15 | +$0.52 | +1.2% | TIME_STOP | 5m4s |

**Premarket Notes:**
- BATL was the star trade - entered at $1.24 on premarket_breakout, trailed up to high watermark of $2.90
- Trailing stop activated after target hit, exited at $2.855 for +130% gain
- Follow-up catalyst_momentum entries were small winners riding the same momentum
- Phase regime (PREMARKET_NORMAL) correctly isolated from RTH CHOP regime

### RTH (09:30 - 16:00 ET)
**Trades: 5/5 cap | P&L: -$4.70 | Win Rate: 60%**

| # | Symbol | Strategy | Entry | Exit | Shares | P&L | % | Exit Reason | Hold |
|---|--------|----------|-------|------|--------|-----|---|-------------|------|
| 1 | LUMN | catalyst_momentum | $8.57 | $7.615 | 5 | -$4.78 | -11.1% | HARD_STOP | 3s |
| 2 | BATL | catalyst_momentum | $2.851 | $2.907 | 8 | +$0.44 | +1.9% | TIME_STOP | 5m2s |
| 3 | BATL | catalyst_momentum | $2.907 | $2.860 | 12 | -$0.56 | -1.6% | TIME_STOP | 5m1s |
| 4 | BOXL | catalyst_momentum | $2.050 | $2.052 | 24 | +$0.04 | +0.1% | TIME_STOP | 5m1s |
| 5 | BOXL | catalyst_momentum | $2.052 | $2.058 | 24 | +$0.16 | +0.3% | TIME_STOP | 5m17s |

**RTH Notes:**
- LUMN hard-stopped in 3 seconds - entered at peak and immediately reversed -11%
- Remaining 4 trades were small, all hitting TIME_STOP at 300s (5 min)
- Only 1 trailing stop activation (BATL trade #2) - most trades didn't move enough
- All trades were catalyst_momentum - no strategy diversity despite HOT then CHOP regime

---

## Exit Analysis

| Exit Type | Count | Avg P&L | Notes |
|-----------|-------|---------|-------|
| TRAIL_STOP | 1 | +$64.60 | Premarket BATL - big winner |
| TIME_STOP | 6 | +$0.12 | Dominated exits - trades not reaching targets or stops |
| HARD_STOP | 1 | -$4.78 | LUMN - entered at bad level, immediate reversal |

**Key Observation:** 75% of trades (6/8) exited via TIME_STOP. This suggests:
- Entry levels may be too close to consolidation zones (not enough directional conviction)
- 300s (5min) max hold may be too short for RTH catalyst_momentum
- Trailing activation threshold (1%) may be too high for small-cap range

---

## Regime & Phase Summary

| Phase | Regime | Duration | Trades | Notes |
|-------|--------|----------|--------|-------|
| PREMARKET | PREMARKET_NORMAL | 07:00-09:30 | 3 | Phase regime correctly isolated from RTH |
| RTH (early) | HOT | 09:30-~12:30 | 5 | Aggressive mode, many signals generated |
| RTH (late) | CHOP | ~12:30-16:00 | 0 | Capped out, would have been slow anyway |
| AFTER_HOURS | AFTER_HOURS_OBSERVE | 16:00+ | 0 | Observe only, no trading |

---

## Validation Mode Summary

| Metric | Value |
|--------|-------|
| Status | **PASSED** |
| Violations | 0 |
| Trades Executed | 5 (RTH only - premarket was on prior server instance) |
| Phase Cap Blocks | RTH: 614 signals blocked after cap |
| Position Limit Blocks | 50 |
| Strategy Disabled Blocks | 217 |
| Time Window Blocks | 0 |
| Data Freshness Checks | 6 passed |
| Halt Block Checks | 6 passed |

**Phase-Scoped Caps (NEW today):**
- PREMARKET: 3/3 used
- RTH: 5/5 used
- AFTER_HOURS: 0/0

---

## System Changes Made Today

### 1. Phase-Aware Regime Architecture (Critical Fix)
- **Problem:** RTH CHOP regime was blocking premarket strategies
- **Solution:** New `MarketPhase` enum + `PhaseRegime` dataclass
- **Result:** Premarket uses PREMARKET_NORMAL (ignores RTH regime), RTH uses computed MASS regime
- **Files:** `market_mode.py`, `pipeline.py`, `mass_risk_governor.py`, `events.py`

### 2. Phase-Scoped Validation Trade Limits (Critical Fix)
- **Problem:** Global `max_trades_per_day=3` consumed by premarket, blocking RTH
- **Solution:** `max_trades_per_phase` dict (PREMARKET: 3, RTH: 5, AFTER_HOURS: 0)
- **Files:** `validation_mode.py`, `main.py`

### 3. Validation Time Window Extension
- **Problem:** RTH window defaulted to 09:30-11:00, blocking afternoon trades
- **Solution:** Extended to 09:30-16:00 ET
- **File:** `main.py`

### 4. RTH Strategy Whitelist Expansion
- **Problem:** Only catalyst_momentum + first_pullback allowed in validation
- **Solution:** Added all 8 non-short RTH strategies
- **File:** `main.py`

### 5. Worklist Gating Fix (from earlier session)
- **Problem:** Force-added symbols weren't in worklist, blocking pipeline
- **Solution:** Force-add endpoint now creates worklist entries as ACTIVE with score=100

### 6. `get_time_et()` Bug Fixes (3 occurrences)
- **Problem:** `get_time_et()` returns string, not datetime - caused AttributeError crashes
- **Solution:** Replaced with `datetime.now(ET)` at all usage points

---

## Observations & Recommendations

### What Worked
1. **Trailing stop on premarket breakout** - BATL +130% captured by letting winner run
2. **Phase isolation** - premarket regime didn't leak into RTH and vice versa
3. **Hard stop** - LUMN loss capped at -$4.78 instead of potentially worse
4. **Validation mode** - 0 violations, all safety checks passed

### What Needs Improvement
1. **Strategy diversity** - All 8 trades were catalyst_momentum or premarket_breakout. No first_pullback, vwap_reclaim, coil_breakout, etc. fired. May need to loosen entry conditions on other strategies.
2. **TIME_STOP dominance** - 6/8 trades exited on time. Consider:
   - Increasing max_hold for catalyst_momentum from 300s to 600s
   - Lowering trail activation threshold from 1% to 0.5%
   - Adding partial profit-taking at target (sell half, trail rest)
3. **Entry quality** - LUMN entered at $8.57 and immediately dumped to $7.62. Need better entry timing or pre-entry spread/momentum checks.
4. **RTH P&L flatness** - 4 trades that were essentially scratches (+0.04 to +0.44). The system is entering but not finding directional moves in 5 minutes.

### For Next Session
- Monitor TIME_STOP exits - if persistent, increase hold time or adjust trailing
- Consider adding max_hold override per strategy via runtime config
- Track whether CHOP regime would have produced better/worse results than HOT
- ONST never warmed up (0 bars) - may need manual investigation

---

## Account Summary

| | Premarket | RTH | Total |
|--|-----------|-----|-------|
| Trades | 3 | 5 | 8 |
| Winners | 3 | 3 | 6 |
| Losers | 0 | 2 | 2 |
| Win Rate | 100% | 60% | 75% |
| Gross P&L | +$65.23 | -$4.70 | **+$60.53** |
| Avg Winner | +$21.74 | +$0.21 | +$10.99 |
| Avg Loser | -- | -$2.67 | -$2.67 |

**Paper Account** | Validation Mode Active | Day 2 of Paper Trading
