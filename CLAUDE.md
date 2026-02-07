# Morpheus Trading System - Claude Context

## System Overview
Morpheus is a momentum trading system for small-cap stocks, consisting of two main components:
- **Morpheus_AI** (C:\Morpheus\Morpheus_AI) - Python backend: signal generation, risk management, order execution
- **Morpheus_UI** (C:\Morpheus\Morpheus_UI) - Electron frontend: trading desktop with docking panels

The human reads tape in Thinkorswim, sees signals in Morpheus UI, confirms entries, and Morpheus executes.

---

## Morpheus_AI (Backend)

### Tech Stack
- Python 3.11+
- FastAPI for REST API and WebSocket
- Schwab API for market data and order execution

### Directory Structure
```
morpheus/
├── ai/            # ML models, scoring
├── broker/        # Schwab integration
├── classify/      # MASS strategy classifier (routing matrix)
├── core/          # Core types, config, mass_config
├── data/          # Market data handlers
├── evolve/        # MASS performance tracking, weight adaptation
├── execution/     # Order execution
├── features/      # Feature engineering
├── integrations/  # MAX_AI_SCANNER integration
├── orchestrator/  # Signal pipeline (central coordinator)
├── regime/        # Market regime detection + MASS regime mapping
├── risk/          # Risk management, position sizing, MASS risk governor
├── scoring/       # Signal scoring, meta-gates
├── server/        # FastAPI server, WebSocket (monolith mode)
├── services/      # Background services
├── spine/         # NATS Data Spine (multi-process architecture)
├── strategies/    # Trading strategies (original + 7 MASS strategies)
├── structure/     # MASS structure analyzer (A/B/C grading)
└── supervise/     # MASS AI supervisor (decay detection)
```

### Key Endpoints
- `GET /api/market/status` - Market data availability
- `GET /api/market/candles/{symbol}` - OHLCV candle data
- `GET /api/market/quotes` - Real-time quotes
- `GET /api/pipeline/status` - Pipeline diagnostics + MASS status
- `POST /api/trading/confirm-entry` - Human confirms signal
- `POST /api/trading/cancel-order` - Cancel order
- `GET /api/mass/status` - MASS system status (regime, classifier, supervisor)
- `GET /api/mass/performance` - Per-strategy performance metrics
- `GET /api/mass/weights` - Current strategy weights
- `POST /api/mass/supervisor/check` - Trigger AI supervisor health check
- `WS /ws/events` - Real-time event stream

### Event Types
- `REGIME_DETECTED` - Market regime change
- `STRUCTURE_CLASSIFIED` - MASS structure grade assigned (A/B/C)
- `STRATEGY_ASSIGNED` - MASS classifier routing result
- `SIGNAL_CANDIDATE` - New trading signal
- `SIGNAL_SCORED` - AI confidence score
- `META_APPROVED/REJECTED` - Gate decision
- `RISK_APPROVED/VETO` - Risk check result
- `ORDER_SUBMITTED/CONFIRMED/FILL_RECEIVED` - Order lifecycle
- `POSITION_UPDATE` - Position changes
- `SUPERVISOR_ALERT` - AI supervisor warning/critical alert
- `FEEDBACK_UPDATE` - Trade outcome recorded
- `STRATEGY_WEIGHT_ADJUSTED` - Strategy weight changed

### Running
```bash
cd C:\Morpheus\Morpheus_AI
python -m morpheus.server.main
```
Runs on http://localhost:8010

---

## Morpheus_UI (Frontend)

### Tech Stack
- Electron + React + TypeScript
- Vite for bundling
- GoldenLayout v2 for docking panels
- LightweightCharts v4 for charting
- Zustand for state management

### Directory Structure
```
src/app/
├── panels/      # Panel components
├── components/  # Shared components (ChainBar, StatusBar)
├── hooks/       # Custom hooks
├── utils/       # Utilities (chartIndicators)
├── store/       # Zustand store
└── morpheus/    # API client, WebSocket client
electron/        # Main process, preload
```

### Panel Types
- **ChartPanel** - Candlesticks with EMA 9/20, VWAP, Volume, MACD
- **DecisionSupportPanel** - Signal confirmation UI
- **OrdersPanel** - Open orders with filters
- **PositionsPanel** - Current positions
- **ExecutionsPanel** - Trade history
- **WatchlistPanel** - Symbol watchlist
- **EventStreamPanel** - Raw event log
- **MorpheusDecisionPanel** - Decision chain visualization
- **TradingControlsPanel** - Mode switching, kill switch

### Chain System
Sterling Trader Pro style linking - 8 colored chains that sync symbol across panels:
- Each chain has: symbol, timeframe, color
- Charts now have independent timeframes for multi-timeframe analysis

### Running
```bash
cd C:\Morpheus\Morpheus_UI
npm run dev
```

---

## Recent UI Changes (This Session)

### Chart Panel Enhancements
- **Indicators**: EMA 9 (grey), EMA 20 (blue), VWAP (yellow-orange solid), Volume, MACD
- **MACD colors**: Histogram green/red (above/below zero), MACD line blue, Signal yellow
- **Resizable panels**: Draggable dividers between price/volume/MACD
- **Independent timeframes**: Each chart has own timeframe selector

### Orders Panel
- Status mapping: Working, Open, Partial, Filled, Canceled, Rejected
- Filter tabs: Active | Filled | Closed | All
- Color-coded badges

### Decision Support Panel
- Compact 3-row layout (state + countdown, symbol + direction + reasons, confirm button)

### Layout Management
- Save/Load/Delete layouts via ChainBar menu
- Persistence in localStorage

---

## Known Issues
- Some pre-existing TypeScript errors in codebase (non-blocking)

---

## Development Notes

### API Data Format
Candles: `{ time: unix_timestamp, open, high, low, close, volume }`

### Indicator Calculations (chartIndicators.ts)
- `calculateEMA(closes, period)` - Exponential moving average
- `calculateVWAP(candles)` - Volume-weighted average price (resets daily)
- `calculateMACD(closes, fast, slow, signal)` - MACD with histogram

### WebSocket Events
Events come through with structure:
```typescript
{
  event_type: string,
  timestamp: string,
  symbol?: string,
  payload: Record<string, unknown>
}
```

---

### NATS Data Spine (2026-01-31)
Multi-process architecture replacing monolith HTTP polling with NATS pub/sub.

**Problem Solved:**
- 25,496 scanner HTTP calls/36min → ~4/min (15s intervals via NATS)
- 424 Schwab position polls → event-driven via NATS
- Single-process bottleneck → 5 isolated processes

**Architecture:**
```
Schwab Feed → NATS → AI Core → NATS → UI Gateway → Morpheus_UI
Scanner Feed → NATS ↗                    ↗ Replay Logger
```

**Spine Directory (`morpheus/spine/`):**
```
├── __init__.py          # Package docstring
├── __main__.py          # python -m morpheus.spine
├── schemas.py           # MsgPack message types (QuoteMsg, OHLCMsg, etc.)
├── nats_client.py       # Async NATS wrapper with auto-reconnect
├── config_store.py      # File-backed spine_config.json
├── schwab_feed.py       # Service 1: Schwab WebSocket → NATS
├── scanner_feed.py      # Service 2: MAX_AI_SCANNER → NATS (interval-based)
├── ai_core.py           # Service 3: NATS → MASS Pipeline → NATS
├── ui_gateway.py        # Service 4: NATS → REST/WebSocket (backward compat)
├── replay_logger.py     # Service 5: NATS → JSONL logs
├── launcher.py          # Process orchestrator
└── spine_config.json    # Configuration
```

**NATS Topics:**
| Topic | Lane | Description |
|-------|------|-------------|
| md.quotes.{sym} | Firehose | Real-time quote ticks |
| md.ohlc.1m.{sym} | Aggregated (1hr) | 1-minute OHLCV bars |
| scanner.context.{sym} | Aggregated (1hr) | Scanner context (every 15s) |
| scanner.alerts | Firehose | Trading halts |
| scanner.discovery | Firehose | New symbol discoveries |
| ai.signals.{sym} | Decisions (24hr) | Trading signals |
| ai.structure.{sym} | Decisions (24hr) | MASS structure grades |
| ai.regime | Decisions (24hr) | Regime changes |
| bot.telemetry.{svc} | Decisions (24hr) | Service health |
| bot.positions | Decisions (24hr) | Position updates |

**Running (Spine Mode):**
```bash
# Prerequisites: NATS server + nats-py + msgpack
choco install nats-server    # or download from nats.io
pip install nats-py msgpack

# Start everything (NATS + 5 services)
cd C:\Morpheus\Morpheus_AI
.\start_spine.ps1

# Or manually:
nats-server -js -m 8222     # Start NATS with JetStream
python -m morpheus.spine     # Start all services

# Start specific services only:
python -m morpheus.spine.launcher --services schwab_feed,ai_core,ui_gateway
```

**Running (Monolith Mode - unchanged):**
```bash
cd C:\Morpheus\Morpheus_AI
python -m morpheus.server.main
```

**Ports:**
- Monolith server: port 8010 (default)
- Spine UI Gateway: port 8011 (can run simultaneously with monolith)
- NATS server: port 4222
- UI connects to port via `VITE_API_PORT` env var (defaults to 8010)

**Running UI Against Spine:**
```bash
cd C:\Morpheus\Morpheus_UI
copy .env.spine .env.local   # Sets VITE_API_PORT=8011
npm run dev
```

**Backward Compatibility:**
- UI Gateway serves identical REST/WebSocket API (same endpoints as monolith)
- Monolith and spine can run simultaneously on different ports
- Monolith server still works independently on port 8010

---

## Next Steps / Ideas
- Indicator toggle UI
- Alert system for signals
- Position sync from Schwab (sync existing positions from TOS)
- MASS Phase 3: AI supervisor reinforcement learning
- MASS Phase 4: Cross-market adaptation, multi-asset support
- Replace StubScorer with ML-based scorer
- UI panel for MASS status/performance/weights

## Completed Fixes

### Schwab Token Independence (2026-01-20)
Morpheus now manages its own Schwab token independently:
- Token stored locally at `./tokens/schwab_token.json` (no longer shares with IBKR_Algo_BOT)
- Server uses `external_refresh=False` - Morpheus handles token refresh on-demand
- At startup, if token is expired, it auto-refreshes using the refresh_token
- No background thread needed - refresh happens before API calls when token expires

### Schwab OAuth Fix (2026-01-21)
Fixed OAuth authentication flow to match Schwab's requirements:
- Removed `scope=readonly` parameter from OAuth URL (schwab_auth.py line 150-155)
- Removed inline scope param from server OAuth URL generation (main.py line 822-826)
- Changed redirect URI to uppercase `HTTPS://127.0.0.1:6969` in .env (Schwab requires exact case match)

### Intraday Chart Data Fix (2026-01-21)
Fixed chart panel to correctly request intraday data from Schwab API:
- Schwab API requires `periodType='day'` (lookback period) with `frequencyType='minute'` (bar size)
- Previous code incorrectly sent `periodType='minute'` which returned empty
- Updated ChartPanel.tsx `getAPIParams()` function with correct param mapping
- Added `frequencyType` parameter to apiClient.ts and server endpoint
- Now 1m, 5m, and 1D timeframes all load correctly

### GoldenLayout Black Screen Fix (2026-01-21)
Fixed black screen crash caused by corrupted layout in localStorage:
- GoldenLayout v2 requires size values as strings (e.g., "20%"), not numbers
- Added `sanitizeLayoutConfig()` function in App.tsx to convert numeric sizes to strings
- Added try-catch around `loadLayout()` with fallback to DEFAULT_LAYOUT
- Removes corrupted layout from localStorage on error

### MAX_AI_SCANNER Integration (2026-01-24)
Integrated MAX_AI_SCANNER as the ONLY source of symbol discovery:
- Created `morpheus/integrations/max_ai_scanner.py` - event mapping, halt tracking, context augmentation
- Scanner events mapped to Morpheus events (HALT, RESUME, GAP_ALERT, MOMO_SURGE, HOD_BREAK)
- `FeatureContext.external` field for scanner context (READ-ONLY)
- Pipeline fetches scanner context and attaches to feature context
- Removed Schwab movers fallback - scanner is sole discovery source

### Premarket Mode + Strategy Gating (2026-01-24)
Added trading window awareness and strategy filtering:

**Trading Windows (Eastern Time):**
| Mode | Time | active_trading | observe_only |
|------|------|----------------|--------------|
| PREMARKET | 07:00-09:30 | True | False |
| RTH | 09:30-16:00 | False | True |
| OFFHOURS | Other | False | True |

**New Files:**
- `morpheus/core/market_mode.py` - MarketMode, get_market_mode(), get_time_et()
- `morpheus/strategies/premarket_observer.py` - PremarketStructureObserver (OBSERVE-only)

**Strategy Gating:**
- All strategies have `allowed_market_modes` property (default: RTH only)
- Strategies filtered by market mode before execution
- `SignalMode` enum: ACTIVE (actionable) vs OBSERVED (logged only)
- OBSERVED signals skip scoring/gate/risk pipeline
- New `SIGNAL_OBSERVED` event type

**API Status:**
`GET /api/pipeline/status` now returns:
```json
{
  "market_mode": "PREMARKET",
  "active_trading": true,
  "observe_only": false,
  "time_et": "2026-01-24T07:30:00-05:00",
  "strategies_for_current_mode": ["PremarketStructureObserver"]
}
```

**PremarketStructureObserver:**
Classifies premarket behavior without generating actionable signals:
- GAP_UP_HOLDING, GAP_UP_FADING
- GAP_DOWN_HOLDING, GAP_DOWN_RECOVERING
- FLAT_CONSOLIDATING, HIGH_VOLATILITY
- Emits tags: structure:*, gap:*, volume:*, scanner:*

### MASS v1.0 - Morpheus Adaptive Strategy System (2026-01-31)
Full adaptive multi-strategy trading system implementation.

**Architecture:**
```
Scanner → Structure → Classifier → Regime → Strategies → Scoring → Gate → Risk → Human Confirm
```

**Pipeline stages added to `morpheus/orchestrator/pipeline.py`:**
- Stage 2.3: MASS Regime Mapping (HOT/NORMAL/CHOP/DEAD)
- Stage 2.5: Structure Analysis (A/B/C quality grading)
- Stage 2.7: Strategy Classification (routes to optimal strategy)
- Backward compatible: `MASSConfig(enabled=False)` falls back to original StrategyRouter

**Phase 1 - Structure + Classifier + Strategies:**

*Structure Analyzer (`morpheus/structure/`):*
- Grades symbols A/B/C based on 100-point rubric
- Scoring: VWAP position (20), EMA stacking (20), trend slope (15), S/R proximity (15), liquidity (15), volume (15)
- A >= 75, B >= 50, C < 50
- C-grade symbols filtered out by classifier

*Strategy Classifier (`morpheus/classify/`):*
- Routes symbols to optimal strategy based on structure + regime + scanner context
- Each strategy scored 0-1 for fit; top N assigned (default max 2)
- Configurable weights per strategy via `ClassifierConfig`
- Mode-aware: only PMB + CAT in PREMARKET, all others in RTH

*7 New MASS Strategies (`morpheus/strategies/`):*
| Code | Strategy | File | Mode | Direction | Key Conditions |
|------|----------|------|------|-----------|----------------|
| PMB | Premarket Breakout | `premarket_breakout.py` | PREMARKET | LONG | Gap>3%, above VWAP, RVOL>2x, RSI 50-75 |
| CAT | Catalyst Momentum | `catalyst_momentum.py` | RTH+PM | LONG | Scanner>70, RVOL>3x, bullish EMA stack |
| D2 | Day-2 Continuation | `day2_continuation.py` | RTH | LONG | Gap holding, above VWAP, RSI 45-65 |
| COIL | Coil Breakout | `coil_breakout.py` | RTH | LONG | BB squeeze, low vol, volume expanding |
| SQZ | Short Squeeze | `short_squeeze.py` | RTH | LONG | RVOL>4x, RSI>60, bullish stack |
| FADE | Gap Fade | `gap_fade.py` | RTH | SHORT | Gap>5% fading, below VWAP, MACD neg |
| SCALP | Order Flow Scalp | `order_flow_scalp.py` | RTH | LONG/SHORT | Spread<0.2%, RVOL>2x, micro-trend |

All strategies conform to existing `Strategy` ABC in `morpheus/strategies/base.py`.
Aggregated via `get_mass_strategies()` in `mass_strategies.py`.

*Configuration (`morpheus/core/mass_config.py`):*
```python
MASSConfig(
    enabled=True,          # Master switch
    structure=StructureConfig(),
    classifier=ClassifierConfig(),
    strategy_risk_overrides={...},  # Per-strategy risk %
    regime_mapping_enabled=False,   # Phase 2 flag
    feedback_enabled=False,         # Phase 2 flag
    supervisor_enabled=False,       # Phase 2 flag
)
```

**Phase 2 - Regime + Risk + Feedback + Supervisor:**

*MASS Regime Mapper (`morpheus/regime/mass_regime.py`):*
- Maps existing 3-component regime to MASS modes
- HOT: trending + high vol + strong momentum → aggression 1.5x, max 5 positions
- NORMAL: trending + normal vol → aggression 1.0x, max 3
- CHOP: ranging + neutral → aggression 0.7x, max 2
- DEAD: low vol + ranging + neutral → aggression 0.5x, no trading

*MASS Risk Governor (`morpheus/risk/mass_risk_governor.py`):*
- Wraps existing risk chain: MASS → RoomToProfit → StandardRisk
- Per-trade risk: 0.25% (vs standard 1%)
- Daily loss limit: 1.5%
- Symbol cooldown: 15 minutes between signals
- Regime-based position count caps
- 3 new VetoReasons: SYMBOL_COOLDOWN, REGIME_POSITION_LIMIT, STRATEGY_RISK_EXCEEDED

*Performance Tracking (`morpheus/evolve/`):*
- `PerformanceTracker`: records TradeOutcome to JSONL, computes win rate/expectancy/PF per strategy
- `StrategyWeightManager`: auto-adjusts weights [0.3, 2.0] based on win rate, profit factor, loss streaks
- Pushes updated weights to classifier

*AI Supervisor (`morpheus/supervise/`):*
- Detects strategy decay (win rate drop >15% from baseline)
- Detects loss streaks (5+ consecutive losses)
- Detects low profit factor (<0.8)
- Auto-actions: reduce weight on warning, near-disable on critical
- Emits SUPERVISOR_ALERT events

**FeatureContext additions:**
- `structure_grade: str` (A/B/C)
- `structure_score: float` (0-100)
- `structure_data: dict` (full structure breakdown)
- Helper: `update_feature_context_with_structure()`

**11 total registered strategies:**
4 original (PremarketStructureObserver, FirstPullback, HODContinuation, VWAPReclaim) + 7 MASS

### Shadow Mode + Ignition Gate + Live Validation (2026-02-07)

**Runtime Modes:** SHADOW (2 trades/$10) → MICRO (5 trades/$50) → LIVE (no caps)

**Ignition Gate (Pipeline Stage 5.5):** 9 fail-closed momentum checks from Playbook:
- score>=60, nofi>=0.2428, l2>=0.5498, spread<=0.0243, confidence>=0.557
- RTH cooldown 120s, conflicting signals, declining score (3 drops), daily loss limit 2%
- No momentum data → REJECT. Gate disabled → pass all.

**Events:** IGNITION_APPROVED, IGNITION_REJECTED

**Reporting:**
- `LiveValidationLogger`: Rolling monthly markdown+JSONL in `D:/AI_BOT_DATA/logs/`
- `DailyReviewGenerator`: EOD summary comparing live vs playbook baseline (42.3% WR, +34 bps)
- `ScaleReadinessReport`: Go/No-Go after 5 sessions (WR>=32%, PF>=1.5, DD<=5%, discipline>=90%)

**API:**
- `POST /api/reports/daily-review` - Generate daily validation summary
- `POST /api/reports/scale-readiness` - Generate Go/No-Go report
- `GET /api/pipeline/status` - Now includes `runtime_mode`, `ignition_gate`

**Key Files:**
- `morpheus/scoring/ignition_gate.py`
- `morpheus/reporting/live_validation_logger.py`
- `morpheus/reporting/daily_review.py`
- `morpheus/reporting/scale_readiness.py`
