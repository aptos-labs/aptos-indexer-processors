from prometheus_client import Counter, Gauge

PROCESSED_TRANSACTIONS_COUNTER = Counter(
    "indexer_processor_processed_transactions",
    "Number of transactions processed",
    ["processor_name"],
)

LATEST_PROCESSED_VERSION = Gauge(
    "indexer_processor_latest_version",
    "Latest processed version",
    ["processor_name"],
)
