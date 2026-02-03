# Morpheus_AI Session Report - 2026-02-02

## Executive Summary

First full day of MASS pipeline paper trading. 187 buy orders executed with ZERO sells due to missing exit logic. Schwab token conflict between Morpheus_AI and IBKR_Algo_BOT_V2 caused 401 errors after server restart. Exit management system was implemented but could not be validated due to token issue.

---

## ISSUE 1: Schwab Token Conflict Between Bots (CRITICAL - BLOCKING)

### Problem
Both Morpheus_AI and IBKR_Algo_BOT_V2 share the **same Schwab OAuth credentials** (`client_id=iYa2563asjgdr2RAYpJxAcATc1yPkzEB`). When either bot refreshes its token, Schwab invalidates the other bot's refresh token, causing 401 Unauthorized on all API calls.

### Evidence
- Morpheus token file: `C:\Morpheus\Morpheus_AI\tokens\schwab_token.json`
- IBKR bot token file: `C:\ai_project_hub\store\code\IBKR_Algo_BOT_V2\schwab_token.json`
- Both use identical `client_id` and `client_secret` (from .env files)
- JWT decode of Morpheus id_token: issued 2026-02-02 07:49:42, expired 2026-02-02 08:49:42
- Server stderr shows continuous 401 errors after restart at 09:22 AM

### Timeline
1. 07:49 AM - Morpheus started, token refreshed successfully, trading began
2. ~09:15 AM - Server killed for restart (to activate new exit management code)
3. 09:22 AM - Server restarted, but IBKR bot had already refreshed the shared token
4. 09:22+ AM - All Schwab API calls return 401 Unauthorized
5. Token auto-refresh fails because Morpheus's refresh_token was orphaned

### Root Cause Detail
- Schwab OAuth2 spec: only ONE active refresh_token per client_id at a time
- When Bot A refreshes, Schwab issues a new refresh_token and invalidates the old one
- Bot B still holds the old (now invalid) refresh_token
- Bot B's next refresh attempt fails -> permanent 401 until manual re-auth

### Additional Bug: Missing `issued_at` in Token File
- The token file sometimes lacks the `issued_at` field
- `schwab_auth.py` `TokenData.from_dict()` defaults to `time.time()` when missing
- This makes an expired token appear valid (expires_at = now + 1800s)
- Auto-refresh at startup never triggers because `is_expired` returns False
- Location: `schwab_auth.py:87` - `issued_at=data.get("issued_at", time.time())`

### Additional Bug: No 401 Retry in Morpheus
- IBKR bot has 401 retry logic in `_make_request()` (schwab_market_data.py:169-212)
- Morpheus has NO 401 retry - if token is stale, request fails permanently
- Location: `schwab_market.py` - `_get_headers()` and all request methods

### Proposed Solutions (Need Consensus Between Both Bots)

**Option A: Separate Schwab Apps (RECOMMENDED)**
- Create a second Schwab developer app at https://developer.schwab.com/
- Each bot gets its own client_id/client_secret
- Pros: Clean separation, no conflicts, no coordination needed
- Cons: Requires creating new app (may take approval time)

**Option B: Shared Token File**
- Both bots read/write to a single shared token file path
- Add file locking to prevent race conditions
- Whoever refreshes writes the new token; the other reads it
- Pros: Works with single client_id
- Cons: Race conditions, file locking complexity, tight coupling

**Option C: Token Coordinator Service**
- Small service that owns the token and serves it to both bots via local HTTP
- Single point of refresh, both bots query for current token
- Pros: Clean architecture, no race conditions
- Cons: Another service to run

**Option D: 401 Retry + Mutual Awareness (MINIMUM FIX)**
- Add 401 retry to Morpheus (like IBKR bot already has)
- On 401, force-refresh and retry once
- Both bots tolerate the other's refresh
- Pros: Quick to implement
- Cons: Refresh storms possible, doesn't prevent the root cause

### Files Involved
| File | Bot | Role |
|------|-----|------|
| `morpheus/broker/schwab_auth.py` | Morpheus | Token lifecycle, refresh, save |
| `morpheus/broker/schwab_market.py` | Morpheus | API calls (no 401 retry) |
| `morpheus/server/main.py:336-352` | Morpheus | Startup token refresh |
| `tokens/schwab_token.json` | Morpheus | Token storage |
| `.env` (SCHWAB_CLIENT_ID) | Morpheus | Credentials |
| `schwab_market_data.py` | IBKR Bot | Token lifecycle + 401 retry |
| `schwab_token.json` | IBKR Bot | Token storage (no issued_at) |
| `.env` (SCHWAB_APP_KEY) | IBKR Bot | Same credentials |

---

## ISSUE 2: No Exit Logic (FIXED - Needs Validation)

### Problem
187 paper buy orders executed, ZERO sells. Strategies compute stop/target levels (`invalidation_reference`, `target_reference`) but nothing monitored prices or triggered exits.

### Evidence
- Event log: 187 ORDER_SUBMITTED (all buys), 0 ORDER_SUBMITTED (sells)
- 16 unique symbols traded, many with repeated buys (DKI: 186,300 shares across 20 fills)
- Total notional: $1.47M on $100K paper account
- Unrealized P&L: -$5,534 (-0.38%)
- AccountState was hardcoded: positions=0, exposure=0, pnl=0

### Fix Implemented (2026-02-02)
Created `morpheus/execution/paper_position_manager.py` - comprehensive exit management:

**Exit Types (priority order):**
1. Kill switch - exit all positions immediately
2. Hard stop - price <= stop_price (from strategy's invalidation_reference)
3. Trailing stop - price drops 1.5% from high watermark (activated after target hit)
4. Target hit - activates trailing mode (does NOT exit immediately)
5. Time stop - elapsed > max_hold_seconds (per-strategy: SCALP=120s, PMB/CAT/SQZ/COIL/FADE=300s, D2=600s)
6. Regime exit - regime changes to DEAD

**Trailing Stop Design (from IBKR_Algo_BOT_V2 performance data):**
- IBKR bot trailing stop: 85% win rate (best performer)
- IBKR bot hard stop: 0% win rate (fallback only)
- Morpheus implementation: target hit activates trailing at 1.5% from high watermark
- This lets winners run while protecting profits

**Position-Aware AccountState:**
- `get_account_state()` returns real position count, exposure, daily P&L
- Pipeline risk checks now see actual portfolio state
- Regime position limits actually enforced (was always 0 before)

### Files Changed
| File | Change |
|------|--------|
| `morpheus/execution/paper_position_manager.py` | NEW - ~480 lines, full exit manager |
| `morpheus/risk/base.py` | Added `POSITION_ALREADY_OPEN` to VetoReason |
| `morpheus/server/main.py` | Wired position manager into event flow + auto-confirm |
| `morpheus/orchestrator/pipeline.py` | Position-aware AccountState, signal blocking |
| `morpheus/risk/mass_risk_governor.py` | Added position check in evaluate() |

### Status: IMPLEMENTED, NOT VALIDATED
- Code compiles and imports verified
- Server started successfully with new code
- Could not validate exit behavior because Schwab token expired (Issue 1)
- Needs live market data to test exits firing

---

## ISSUE 3: Unlimited Re-Entry on Same Symbol (FIXED)

### Problem
60-second symbol cooldown expires and bot re-buys same symbol endlessly. DKI was bought 20 times accumulating 186,300 shares.

### Fix: 3-Layer Defense-in-Depth
1. **Pipeline cooldown** (`pipeline.py:_check_signal_cooldown`) - blocks signals while position open (cheapest check, first gate)
2. **MASS risk governor** (`mass_risk_governor.py:evaluate`) - formal POSITION_ALREADY_OPEN veto with event logging
3. **Auto-confirm check** (`main.py`) - blocks order creation if position manager reports open position

### Status: IMPLEMENTED, NOT VALIDATED

---

## ISSUE 4: Hardcoded AccountState (FIXED)

### Problem
`_create_stub_account_state()` in pipeline.py always returned:
- total_equity = 100000
- open_position_count = 0
- total_exposure = 0
- daily_pnl = 0
- daily_pnl_pct = 0

This meant regime position limits, daily loss limits, and exposure checks never triggered.

### Fix
Pipeline now calls `position_manager.get_account_state()` which returns real values from the position book. Falls back to stub only if position_manager is not available.

### Status: IMPLEMENTED, NOT VALIDATED

---

## Trading Activity Summary (Pre-Fix, 2026-02-02)

### Pipeline Funnel
| Stage | Count |
|-------|-------|
| Signals generated | 5,969 |
| Signals scored | 6,429 |
| META_APPROVED | 6,324 |
| RISK_APPROVED | 209 |
| RISK_VETO | 5,953 |
| ORDER_SUBMITTED (buy) | 187 |
| ORDER_FILL_RECEIVED | 187 |
| ORDER_SUBMITTED (sell) | 0 |

### Risk Veto Breakdown
| Reason | Count |
|--------|-------|
| SYMBOL_COOLDOWN | 3,957 |
| STRATEGY_RISK_EXCEEDED | 1,988 |
| insufficient_room_to_profit | 8 |

### Top Symbols by Fill Count
| Symbol | Fills | Total Shares | Avg Price | Notional |
|--------|-------|-------------|-----------|----------|
| DKI | 20 | 186,300 | ~$0.50 | ~$93K |
| FUSE | 18 | ~15,000 | varies | varies |
| (14 others) | 149 | varies | varies | varies |

### Aggregate
- Total notional: ~$1.47M
- Paper account: $100K
- Unrealized P&L: -$5,534 (-0.38%)
- Leverage: ~14.7x (no position limits enforced)

---

---

## ISSUE 5: Max_UI Auth Code Needs Fixing

Max_UI authentication code is broken - needs repair as part of tonight's auth fixes.

---

## Cross-Bot Fix Coordination (Tonight)

### Morpheus_AI Issues
1. Schwab token conflict (shared client_id) - **TOP PRIORITY**
2. Missing `issued_at` in token file
3. No 401 retry logic in schwab_market.py

### IBKR_Algo_BOT_V2 Issues (from their list)
- F1: Autonomous token renewal
- F2: Reload endpoint
- F3: Restart bug
- F4: Scheduled task kill issue
- Token sharing conflict - **TOP PRIORITY**

### Max_UI Issues
- Auth code broken - needs fix

### Design Requirement: Fully Automated OAuth (No User Interaction)

Currently when a refresh token expires (every 7 days) or gets orphaned by the other bot, recovery requires manual steps: open browser, log into Schwab, approve OAuth, copy callback URL back. This is labor-intensive and blocks trading until completed.

**Goal:** Build a headless OAuth automation process that each bot can invoke when its refresh token fails, using stored Schwab login credentials to complete the full OAuth2 authorization code flow without any user interaction.

**Proposed Architecture: Schwab Auto-Auth Service**

```
[Bot detects 401 / refresh failure]
    → calls auto-auth module
    → headless browser (Playwright/Selenium) opens Schwab OAuth URL
    → auto-fills login credentials (stored securely, same creds as Schwab API)
    → auto-approves OAuth consent
    → captures redirect callback with auth code
    → exchanges auth code for access_token + refresh_token
    → saves new token to bot's token file
    → bot resumes API calls
```

**Implementation Options:**

**Option A: Shared Auto-Auth Service (RECOMMENDED)**
- Single service that all 3 systems (Morpheus_AI, IBKR bot, Max_UI) can call
- Manages ONE set of tokens per client_id
- Exposes local endpoint: `POST /auth/refresh` → returns fresh token
- Runs scheduled token refresh before expiry (e.g., every 25 min for access, every 6 days for refresh)
- Eliminates the conflict entirely - one service, one token, served to all consumers
- Could be a simple FastAPI or Flask app running on a fixed local port

**Option B: Per-Bot Auto-Auth Module**
- Each bot has its own copy of the headless auth logic
- Each bot independently refreshes when needed
- Still needs token coordination to avoid orphaning (same problem as today)
- Simpler per-bot, but doesn't solve the shared-credential conflict

**Option C: Hybrid - Shared Service + Separate Client IDs**
- Create a second Schwab app for one of the bots
- Auto-auth service manages both client_ids
- No token conflicts possible
- Each bot gets its own independent token lifecycle

**Credential Storage:**
- Schwab login username/password stored in encrypted local config or `.env`
- Same credentials used for browser login to schwab.com
- Only accessed by the auto-auth module, never transmitted externally
- Could use Windows Credential Manager or a simple encrypted file

**Dependencies:**
- `playwright` (preferred - faster, more reliable than Selenium) or `selenium`
- Headless Chromium/Chrome
- Schwab login credentials (username + password)

**Scheduled Refresh (Proactive, Not Reactive):**
- Access token: refresh every 25 minutes (expires at 30)
- Refresh token: full re-auth every 6 days (expires at 7)
- On failure: retry 3x with backoff, then alert user
- Keeps tokens perpetually fresh - bots never see a 401

### Recommended Approach
Upload both bots' issue reports to a shared chat so they can agree on a single token/auth strategy. All three systems (Morpheus_AI, IBKR_Algo_BOT_V2, Max_UI) need to be aligned on whichever approach is chosen.

---

## Action Items for Tomorrow (2026-02-03)

### Must Fix Before Trading
1. **Resolve Schwab token conflict** - needs consensus between both bots
2. **Validate exit management** - confirm exits fire on live data
3. **Validate re-entry blocking** - confirm no duplicate buys

### Should Monitor
4. AccountState shows real position count in pipeline status API
5. Trailing stop activations after target hit
6. Time stop behavior at strategy-specific limits
7. P&L tracking accuracy

### Nice to Have
8. Add 401 retry logic to Morpheus regardless of token fix
9. Always persist `issued_at` in token file
10. Add position manager status to `/api/pipeline/status` endpoint
