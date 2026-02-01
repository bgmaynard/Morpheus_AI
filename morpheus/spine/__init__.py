"""
Morpheus Data Spine - NATS-based message bus for decoupled services.

The spine replaces direct HTTP polling and in-process callbacks with
a publish/subscribe architecture using NATS as the message transport.

Services:
- schwab_feed:    Schwab WebSocket → NATS quotes/bars/positions
- scanner_feed:   MAX_AI_SCANNER → NATS scanner context/alerts/discovery
- ai_core:        NATS → MASS pipeline → NATS signals/structure/regime
- ui_gateway:     NATS → REST/WebSocket for Morpheus_UI
- replay_logger:  NATS → JSONL event logs for replay

Lanes:
1. Tick Firehose  (plain pub/sub):  md.quotes.*, scanner.alerts
2. Aggregated     (JetStream 1hr):  md.ohlc.*, md.features.*, scanner.context.*
3. Decisions      (JetStream 24hr): ai.*, bot.*
"""
