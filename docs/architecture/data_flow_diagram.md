# Data Flow Diagram

The deliverable data-flow diagram, kept on its own so it can be exported
cleanly as an image.

**To export as PNG or PDF:** paste the block below into
<https://mermaid.live>, then *Actions → PNG* / *SVG*. Save the result next to
this file as `data_flow_diagram.png` and it will render inline in the README.
(A Mermaid diagram is also valid draw.io input: *Arrange → Insert → Advanced →
Mermaid*.)

```mermaid
flowchart TD
    START(["Simulation tick<br/>every GENERATION_INTERVAL_SECONDS"])

    G["<b>1. Generate</b><br/>HeartRateEvent per customer<br/>event_id · customer_id · timestamp · heart_rate"]
    P["<b>2. Produce</b><br/>serialize to JSON<br/>key = customer_id, acks = all"]
    K[["<b>3. Kafka topic</b><br/>heart-rate-readings<br/>ordered per customer"]]
    C["<b>4. Consume</b><br/>poll, manual offset commit"]
    V{"<b>5. Validate</b><br/>parseable? fields present?<br/>timestamp tz-aware?<br/>20 ≤ bpm ≤ 250?"}
    DROP["<b>Discard</b><br/>log reason, count as invalid<br/>pipeline continues"]
    A{"<b>6. Classify</b><br/>compare against<br/>ANOMALY_LOW / HIGH_THRESHOLD"}
    N["status = NORMAL<br/>is_anomaly = false"]
    L["status = LOW<br/>is_anomaly = true"]
    H["status = HIGH<br/>is_anomaly = true"]
    B["<b>7. Buffer</b><br/>flush at batch_size or on idle poll"]
    W["<b>8. Persist</b><br/>INSERT ... ON CONFLICT (event_id)<br/>DO NOTHING"]
    DB[("<b>PostgreSQL</b><br/>heart_rate_readings")]
    OFF["<b>9. Commit offsets</b><br/>only after a successful write"]

    Q["SQL analytics<br/>sql/queries/"]
    D["Streamlit dashboard<br/>read-only"]

    START --> G --> P --> K --> C --> V
    V -->|invalid| DROP
    V -->|valid| A
    A --> N
    A --> L
    A --> H
    N --> B
    L --> B
    H --> B
    B --> W --> DB
    W --> OFF
    DB --> Q
    DB --> D

    classDef store fill:#0b7285,stroke:#065666,color:#ffffff
    classDef drop fill:#a61e4d,stroke:#7d1436,color:#ffffff
    classDef bus fill:#5f3dc4,stroke:#432aa0,color:#ffffff
    class DB,K store
    class DROP drop
    class K bus
```

## Reading the diagram

| Step | Module | Guarantee it provides |
|---|---|---|
| 1 Generate | `generator/` | Every emitted event is structurally valid, including deliberate anomalies |
| 2 Produce | `producer/` | `acks=all` + idempotence: no silent message loss |
| 3 Kafka | — | Per-customer ordering, via `customer_id` as partition key |
| 4 Consume | `consumer/` | At-least-once delivery; offsets under our control |
| 5 Validate | `validation/` | Unusable data never reaches storage; **alarming data does** |
| 6 Classify | `processing/` | Every stored reading carries a status |
| 7 Buffer | `consumer/` | Throughput without stranding low-volume data |
| 8 Persist | `database/` | Idempotent write — replay cannot duplicate rows |
| 9 Commit | `consumer/` | Crash before this point replays rather than loses |
