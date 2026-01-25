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
├── ai/          # ML models, scoring
├── broker/      # Schwab integration
├── core/        # Core types, config
├── data/        # Market data handlers
├── execution/   # Order execution
├── features/    # Feature engineering
├── regime/      # Market regime detection
├── risk/        # Risk management, position sizing
├── scoring/     # Signal scoring
├── server/      # FastAPI server, WebSocket
├── services/    # Background services
└── strategies/  # Trading strategies
```

### Key Endpoints
- `GET /api/market/status` - Market data availability
- `GET /api/market/candles/{symbol}` - OHLCV candle data
- `GET /api/market/quotes` - Real-time quotes
- `POST /api/trading/confirm-entry` - Human confirms signal
- `POST /api/trading/cancel-order` - Cancel order
- `WS /ws/events` - Real-time event stream

### Event Types
- `REGIME_DETECTED` - Market regime change
- `SIGNAL_CANDIDATE` - New trading signal
- `SIGNAL_SCORED` - AI confidence score
- `META_APPROVED/REJECTED` - Gate decision
- `RISK_APPROVED/VETO` - Risk check result
- `ORDER_SUBMITTED/CONFIRMED/FILL_RECEIVED` - Order lifecycle
- `POSITION_UPDATE` - Position changes

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

## Next Steps / Ideas
- Add more indicators (Bollinger Bands, RSI)
- Indicator toggle UI
- Alert system for signals
- Position sync from Schwab (sync existing positions from TOS)

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
