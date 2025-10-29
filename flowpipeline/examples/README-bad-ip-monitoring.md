### Bad IP Monitoring with Prometheus and Grafana

Steps:

- Start the pipeline with Prometheus exporter enabled in `flowpipeline/config.yml`.
- Configure Prometheus to scrape the exporter using `examples/export/prometheus_scrape.yml`.
- In Grafana, add a Prometheus data source pointing to your Prometheus server.
- Import `examples/export/grafana-bad-ips-dashboard.json` and set the Prometheus data source.

Notes:

- Metrics are exposed at `http://localhost:9090/metrics` and flow data at `/flowdata`.
- We include `Tid` and `Note` labels; bad IPs are tagged with `Tid=65001` and `Note=bad_ip`.
- Dashboard panels use `flow_bits` converted to bps via `rate()`.

