This is effective a cron job that checks key metrics for Indexer and publishes them to Prometheus. 

## How to run
```
cargo run --release -- -c config.yaml
```
You should also be able to see metrics moving by navigating to `0.0.0.0:{health_check_port}/metrics`
