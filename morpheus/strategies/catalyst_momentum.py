"""
MASS Strategy: Catalyst Momentum (CAT)

Rides strong momentum on catalyst-driven stocks (earnings, news, FDA).
Uses scanner context for catalyst identification.

Conditions:
- Strong uptrend or volatile uptrend regime
- Scanner score > 70
- RVOL > 3x
- EMA 9 > EMA 21 (bullish stack)
- Price above VWAP
- RSI 55-80

All evaluation is DETERMINISTIC.
"""

from __future__ import annotations

from morpheus.strategies.base import (
    Strategy,
    StrategyContext,
    SignalCandidate,
    SignalDirection,
)


class CatalystMomentumStrategy(Strategy):
    """Catalyst Momentum - rides strong momentum on catalyst-driven stocks."""

    @property
    def name(self) -> str:
        return "catalyst_momentum"

    @property
    def description(self) -> str:
        return "Momentum entry on catalyst-driven stocks with strong scanner score"

    @property
    def required_features(self) -> tuple[str, ...]:
        return ("ema_9", "ema_21", "vwap", "rsi_14", "relative_volume", "macd_histogram")

    @property
    def compatible_regimes(self) -> tuple[str, ...]:
        return ("trending_up", "volatile_trending_up", "strong_bullish")

    @property
    def allowed_market_modes(self) -> frozenset[str]:
        return frozenset({"RTH", "PREMARKET"})

    def evaluate(self, context: StrategyContext) -> SignalCandidate:
        f = context.features
        price = context.snapshot.last

        # Scanner score check
        scanner_score = f.external.get("scanner_score", 0)
        if not scanner_score or scanner_score < 70:
            return self.create_no_signal(context)

        # RVOL check
        rvol = f.get_feature("relative_volume")
        scanner_rvol = f.external.get("rvol_proxy")
        effective_rvol = max(rvol or 0, scanner_rvol or 0)
        if effective_rvol < 3.0:
            return self.create_no_signal(context)

        # Bullish EMA stack
        ema_9 = f.get_feature("ema_9")
        ema_21 = f.get_feature("ema_21")
        if ema_9 is None or ema_21 is None or ema_9 <= ema_21:
            return self.create_no_signal(context)

        # Above VWAP
        vwap = f.get_feature("vwap")
        if vwap is not None and price <= vwap:
            return self.create_no_signal(context)

        # RSI sweet spot
        rsi = f.get_feature("rsi_14")
        if rsi is not None and (rsi < 55 or rsi > 80):
            return self.create_no_signal(context)

        # Compute reference levels
        atr = f.get_feature("atr_14")
        stop_distance = atr * 1.5 if atr else price * 0.025
        entry_ref = price
        stop_ref = price - stop_distance
        target_ref = price + (stop_distance * 3.0)

        return self.create_signal(
            context,
            direction=SignalDirection.LONG,
            rationale=(
                f"CAT: scanner {scanner_score:.0f}, RVOL {effective_rvol:.1f}x, "
                f"EMA stack bullish, RSI {rsi:.0f}"
            ),
            triggering_features=("ema_9", "ema_21", "vwap", "rsi_14", "relative_volume"),
            tags=("mass", "cat", "catalyst", "momentum", f"scanner:{scanner_score:.0f}"),
            entry_reference=entry_ref,
            invalidation_reference=stop_ref,
            target_reference=target_ref,
        )
