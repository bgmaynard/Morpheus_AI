"""
Market Mode - Trading Window and Session Management.

Defines the active trading windows and market session modes.

PREMARKET ACTIVE WINDOW: 07:00-09:30 ET
- Limit orders only, choppy liquidity
- Only premarket-safe strategies allowed

RTH (Regular Trading Hours): 09:30-16:00 ET
- Currently observe-only (user works during day)
- Full strategy set available when enabled

OFFHOURS: Outside trading windows
- Observe-only mode
- No actionable signals
"""

from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

# Eastern Time zone - canonical for US markets
ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class MarketMode:
    """
    Immutable market mode state.

    Determines what trading actions are allowed at a given time.
    """
    name: str  # "PREMARKET" | "RTH" | "OFFHOURS"
    active_trading: bool  # True only in allowed window
    observe_only: bool  # True when evaluation runs but signals are non-actionable

    def to_dict(self) -> dict:
        """Convert to dict for event payloads."""
        return {
            "market_mode": self.name,
            "active_trading": self.active_trading,
            "observe_only": self.observe_only,
        }


# Pre-defined market modes
PREMARKET = MarketMode(name="PREMARKET", active_trading=True, observe_only=False)
RTH = MarketMode(name="RTH", active_trading=False, observe_only=True)  # Observe-only for now
OFFHOURS = MarketMode(name="OFFHOURS", active_trading=False, observe_only=True)


def get_market_mode(now: datetime | None = None) -> MarketMode:
    """
    Determine the current market mode based on time.

    Trading Windows (Eastern Time):
    - PREMARKET: 07:00-09:30 ET (active trading allowed)
    - RTH: 09:30-16:00 ET (observe-only for now - user works during day)
    - OFFHOURS: All other times (observe-only)

    Args:
        now: Optional datetime to evaluate (defaults to current time in ET)

    Returns:
        MarketMode indicating current session state
    """
    if now is None:
        try:
            from morpheus.core.time_authority import get_time_authority
            now = get_time_authority().now()
        except ImportError:
            now = datetime.now(ET)
    elif now.tzinfo is None:
        # Assume naive datetime is in ET
        now = now.replace(tzinfo=ET)
    else:
        # Convert to ET
        now = now.astimezone(ET)

    t = now.time()

    # Define window boundaries
    pre_open = time(7, 0)
    rth_open = time(9, 30)
    rth_close = time(16, 0)

    if pre_open <= t < rth_open:
        return PREMARKET
    elif rth_open <= t < rth_close:
        # For now, treat RTH as observe-only unless explicitly enabled later.
        # This matches user's operational constraint (works during day).
        return RTH
    else:
        return OFFHOURS


def get_time_et(now: datetime | None = None) -> str:
    """
    Get current time as ET string for logging.

    Args:
        now: Optional datetime (defaults to current time)

    Returns:
        ISO format string in ET timezone
    """
    if now is None:
        try:
            from morpheus.core.time_authority import get_time_authority
            now = get_time_authority().now()
        except ImportError:
            now = datetime.now(ET)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    else:
        now = now.astimezone(ET)

    return now.isoformat()


def is_premarket(now: datetime | None = None) -> bool:
    """Check if currently in premarket window."""
    return get_market_mode(now).name == "PREMARKET"


def is_rth(now: datetime | None = None) -> bool:
    """Check if currently in regular trading hours."""
    return get_market_mode(now).name == "RTH"


def is_trading_allowed(now: datetime | None = None) -> bool:
    """Check if active trading is allowed right now."""
    return get_market_mode(now).active_trading
