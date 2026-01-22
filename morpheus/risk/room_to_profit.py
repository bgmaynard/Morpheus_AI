"""
Room-to-Profit Calculator - Ensure trades can overcome spread and slippage.

A trade must overcome: spread + expected_slippage + commission

This module calculates:
1. Minimum price movement needed to break even
2. Risk/reward ratio accounting for execution costs
3. Whether a trade has sufficient room to profit

Key principle: Never take a trade where execution costs eat most of the profit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExecutionCosts:
    """All costs associated with executing a trade."""

    spread: float  # Bid-ask spread in dollars
    spread_pct: float  # Spread as % of price

    entry_slippage: float  # Expected slippage on entry
    exit_slippage: float  # Expected slippage on exit

    commission: float = 0.0  # Per-share commission (usually $0 for stocks)

    @property
    def total_cost(self) -> float:
        """Total round-trip cost in dollars per share."""
        return self.spread + self.entry_slippage + self.exit_slippage + (self.commission * 2)

    @property
    def total_cost_pct(self) -> float:
        """Total cost as % of entry price."""
        # Spread_pct already accounts for price, but slippage needs conversion
        return self.spread_pct + (self.entry_slippage + self.exit_slippage) / self.spread * self.spread_pct if self.spread > 0 else 0


@dataclass
class RoomToProfit:
    """Room-to-profit analysis for a potential trade."""

    # Trade setup
    symbol: str
    entry_price: float
    stop_price: float
    target_price: float
    direction: str  # 'long' or 'short'

    # Costs
    execution_costs: ExecutionCosts

    # Calculated values
    raw_risk: float  # Entry to stop (before costs)
    raw_reward: float  # Entry to target (before costs)
    net_reward: float  # Reward after costs
    net_risk: float  # Risk after costs (usually same as raw)

    raw_rr_ratio: float  # Raw risk/reward
    net_rr_ratio: float  # Net risk/reward (actual)

    breakeven_move: float  # Minimum move needed to break even
    breakeven_pct: float  # Breakeven as % of entry

    # Verdict
    has_room: bool  # True if trade has sufficient room
    room_score: float  # 0-100 score (higher = more room)
    rejection_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "direction": self.direction,
            "execution_costs": {
                "spread": self.execution_costs.spread,
                "spread_pct": self.execution_costs.spread_pct,
                "entry_slippage": self.execution_costs.entry_slippage,
                "exit_slippage": self.execution_costs.exit_slippage,
                "total": self.execution_costs.total_cost,
            },
            "raw_risk": self.raw_risk,
            "raw_reward": self.raw_reward,
            "net_reward": self.net_reward,
            "net_risk": self.net_risk,
            "raw_rr_ratio": self.raw_rr_ratio,
            "net_rr_ratio": self.net_rr_ratio,
            "breakeven_move": self.breakeven_move,
            "breakeven_pct": self.breakeven_pct,
            "has_room": self.has_room,
            "room_score": self.room_score,
            "rejection_reason": self.rejection_reason,
        }


@dataclass
class RoomToProfitConfig:
    """Configuration for room-to-profit calculations."""

    # Minimum requirements
    min_net_rr_ratio: float = 1.5  # Minimum net R:R after costs
    min_room_score: float = 50.0  # Minimum room score (0-100)
    max_cost_to_reward_pct: float = 30.0  # Max % of reward eaten by costs

    # Slippage assumptions
    default_slippage_pct: float = 0.1  # 0.1% default slippage each way
    high_volatility_slippage_pct: float = 0.25  # Slippage in volatile conditions

    # Spread thresholds
    max_spread_pct: float = 1.5  # Reject if spread > 1.5% of price
    tight_spread_bonus: float = 10.0  # Bonus score for < 0.3% spread
    wide_spread_penalty: float = 20.0  # Penalty for > 1% spread


class RoomToProfitCalculator:
    """
    Calculates whether a trade has sufficient room to profit.

    Usage:
        calc = RoomToProfitCalculator()
        result = calc.calculate(
            symbol="AAPL",
            entry_price=150.00,
            stop_price=148.00,
            target_price=155.00,
            bid=149.98,
            ask=150.02,
        )

        if result.has_room:
            # Trade has sufficient room
        else:
            # Trade blocked: result.rejection_reason
    """

    def __init__(self, config: RoomToProfitConfig | None = None):
        self.config = config or RoomToProfitConfig()

    def calculate(
        self,
        symbol: str,
        entry_price: float,
        stop_price: float,
        target_price: float,
        bid: float,
        ask: float,
        direction: str = "long",
        volatility_multiplier: float = 1.0,
    ) -> RoomToProfit:
        """
        Calculate room-to-profit for a trade setup.

        Args:
            symbol: Trading symbol
            entry_price: Expected entry price
            stop_price: Stop loss price
            target_price: Profit target price
            bid: Current bid price
            ask: Current ask price
            direction: 'long' or 'short'
            volatility_multiplier: Multiply slippage for volatile conditions

        Returns:
            RoomToProfit analysis
        """
        # Calculate spread
        spread = ask - bid if ask > bid else 0.01  # Minimum 1 cent spread
        spread_pct = (spread / entry_price * 100) if entry_price > 0 else 0

        # Calculate slippage based on config and volatility
        base_slippage_pct = self.config.default_slippage_pct * volatility_multiplier
        entry_slippage = entry_price * base_slippage_pct / 100
        exit_slippage = entry_price * base_slippage_pct / 100

        # Build execution costs
        costs = ExecutionCosts(
            spread=spread,
            spread_pct=spread_pct,
            entry_slippage=entry_slippage,
            exit_slippage=exit_slippage,
        )

        # Calculate raw risk and reward
        if direction.lower() == "long":
            raw_risk = entry_price - stop_price
            raw_reward = target_price - entry_price
        else:  # short
            raw_risk = stop_price - entry_price
            raw_reward = entry_price - target_price

        # Ensure positive values
        raw_risk = abs(raw_risk)
        raw_reward = abs(raw_reward)

        # Calculate net values (after costs)
        net_reward = raw_reward - costs.total_cost
        net_risk = raw_risk  # Risk doesn't change with costs

        # Calculate R:R ratios
        raw_rr = (raw_reward / raw_risk) if raw_risk > 0 else 0
        net_rr = (net_reward / net_risk) if net_risk > 0 else 0

        # Calculate breakeven
        breakeven_move = costs.total_cost
        breakeven_pct = (breakeven_move / entry_price * 100) if entry_price > 0 else 0

        # Calculate room score
        room_score, rejection_reason = self._calculate_score(
            spread_pct=spread_pct,
            net_rr=net_rr,
            costs=costs,
            raw_reward=raw_reward,
        )

        # Determine if trade has room
        has_room = (
            room_score >= self.config.min_room_score
            and net_rr >= self.config.min_net_rr_ratio
            and rejection_reason is None
        )

        return RoomToProfit(
            symbol=symbol,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            direction=direction,
            execution_costs=costs,
            raw_risk=raw_risk,
            raw_reward=raw_reward,
            net_reward=net_reward,
            net_risk=net_risk,
            raw_rr_ratio=raw_rr,
            net_rr_ratio=net_rr,
            breakeven_move=breakeven_move,
            breakeven_pct=breakeven_pct,
            has_room=has_room,
            room_score=room_score,
            rejection_reason=rejection_reason,
        )

    def _calculate_score(
        self,
        spread_pct: float,
        net_rr: float,
        costs: ExecutionCosts,
        raw_reward: float,
    ) -> tuple[float, str | None]:
        """
        Calculate room score and determine rejection reason.

        Returns:
            (score, rejection_reason) - rejection_reason is None if acceptable
        """
        score = 50.0  # Start at neutral
        rejection_reason = None

        # Hard rejection: spread too wide
        if spread_pct > self.config.max_spread_pct:
            return 0.0, f"Spread too wide ({spread_pct:.2f}%)"

        # Spread component (±20 points)
        if spread_pct < 0.3:
            score += self.config.tight_spread_bonus
        elif spread_pct > 1.0:
            score -= self.config.wide_spread_penalty

        # R:R component (±30 points)
        if net_rr >= 3.0:
            score += 30
        elif net_rr >= 2.0:
            score += 20
        elif net_rr >= 1.5:
            score += 10
        elif net_rr >= 1.0:
            score -= 10
        else:
            score -= 30
            if net_rr < self.config.min_net_rr_ratio:
                rejection_reason = f"Net R:R too low ({net_rr:.2f})"

        # Cost-to-reward ratio (±20 points)
        if raw_reward > 0:
            cost_to_reward_pct = (costs.total_cost / raw_reward) * 100
            if cost_to_reward_pct > self.config.max_cost_to_reward_pct:
                score -= 20
                rejection_reason = rejection_reason or f"Costs eat {cost_to_reward_pct:.0f}% of reward"
            elif cost_to_reward_pct < 10:
                score += 20
            elif cost_to_reward_pct < 20:
                score += 10

        # Clamp score to 0-100
        score = max(0, min(100, score))

        return score, rejection_reason

    def quick_check(
        self,
        entry_price: float,
        target_price: float,
        spread: float,
        direction: str = "long",
    ) -> bool:
        """
        Quick check if a trade might have room (without full calculation).

        Use this for fast filtering before detailed analysis.
        """
        if direction.lower() == "long":
            potential_reward = target_price - entry_price
        else:
            potential_reward = entry_price - target_price

        # Rule of thumb: reward should be at least 3x the spread
        return potential_reward >= (spread * 3)


def calculate_optimal_target(
    entry_price: float,
    stop_price: float,
    bid: float,
    ask: float,
    min_rr_ratio: float = 2.0,
    direction: str = "long",
) -> float:
    """
    Calculate the minimum target price needed for a given R:R ratio.

    Accounts for spread and slippage to give a realistic target.
    """
    calc = RoomToProfitCalculator()

    # Calculate costs
    spread = ask - bid
    slippage = entry_price * calc.config.default_slippage_pct / 100 * 2  # Both ways
    total_cost = spread + slippage

    # Calculate risk
    if direction.lower() == "long":
        risk = entry_price - stop_price
    else:
        risk = stop_price - entry_price

    risk = abs(risk)

    # Calculate required reward
    # net_reward = raw_reward - total_cost
    # net_rr = net_reward / risk
    # Therefore: raw_reward = (net_rr * risk) + total_cost
    required_raw_reward = (min_rr_ratio * risk) + total_cost

    # Calculate target
    if direction.lower() == "long":
        target = entry_price + required_raw_reward
    else:
        target = entry_price - required_raw_reward

    return round(target, 2)
