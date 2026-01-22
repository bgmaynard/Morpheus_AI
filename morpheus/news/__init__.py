"""
News Module - Catalyst awareness for trading signals.

Provides real-time news monitoring for:
- Earnings announcements
- FDA decisions
- Contract wins
- Analyst upgrades/downgrades
- SEC filings
- PR releases

News context enhances signal quality and helps explain momentum.
"""

from morpheus.news.service import NewsService, NewsItem, NewsCatalyst

__all__ = ["NewsService", "NewsItem", "NewsCatalyst"]
