from prometheus_client import Counter

PROCESSED_TRANSACTIONS_COUNTER = Counter(
    "processed_transactions", "Number of transactions processed"
)
