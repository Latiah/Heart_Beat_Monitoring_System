# Screenshots

Evidence of the pipeline running end-to-end. Capture these after following
the setup steps in the root [README](../../README.md), then commit the images
here — the filenames below are the ones the README expects.

| File | What to capture | How |
|---|---|---|
| `producer_output.png` | Producer logs streaming events | `make run-producer` |
| `consumer_output.png` | Consumer logs showing batches persisted, including at least one `ANOMALY` warning line | `make run-consumer` |
| `database_table.png` | Rows in the table | `make psql`, then `\i sql/queries/recent_readings.sql` |
| `anomaly_summary.png` | Status breakdown and per-customer anomaly rates | `make psql`, then `\i sql/queries/anomaly_summary.sql` |
| `unit_tests.png` | Passing unit tests | `make test-unit` |
| `integration_tests.png` | Passing integration tests | `make test-integration` |
| `dashboard.png` | The Streamlit dashboard with live data | `make dashboard` |

Tip: leave the producer running for a minute or two before the screenshots so
the tables and charts have enough data — and enough anomalies — to be worth
looking at.

For the two long-running processes, redirecting to a file as well as the
terminal gives you a copy-pasteable transcript alongside the image:

```bash
python scripts/run_consumer.py 2>&1 | tee docs/screenshots/consumer_output.txt
```
