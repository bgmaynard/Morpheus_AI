"""
Reporting Module - EOD reports and analysis.

Generates daily reports including:
- Opportunities observed
- Trades executed and blocked (with reasons)
- P&L summary
- Spread/slippage analysis
- Ranked issue list for improvement
"""

from morpheus.reporting.eod_report import EODReportGenerator, EODReport

__all__ = ["EODReportGenerator", "EODReport"]
