"""
Scanner Module - Auto-discovery of trading candidates.

The scanner identifies candidates, not trades. It feeds a dynamic
watchlist based on:
- Relative volume (RVOL)
- Percent change (gap/momentum)
- Price range (small-cap focus)
- Volatility expansion
- Spread behavior
"""

from morpheus.scanner.scanner import Scanner, ScannerConfig, ScanResult

__all__ = ["Scanner", "ScannerConfig", "ScanResult"]
