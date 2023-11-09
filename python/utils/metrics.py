from prometheus_client import Counter, Gauge

PROCESSED_TRANSACTIONS_COUNTER = Counter(
    "indexer_py_processor_processed_transactions", "Number of transactions processed"
)

LATEST_PROCESSED_VERSION = Gauge(
    "indexer_py_processor_latest_processed_version", "Latest processed version"
)

