# **Part 3 – Data Quality and Monitoring**

## **Goals**

The purpose of the data quality and monitoring plan is to ensure that the pipeline is:

* **Correct** (no silent data loss)  
* **Timely** (data arrives within SLA)  
* **Consistent** (schemas and joins remain valid)  
* **Observable** (issues are detected early)

The plan covers **streaming ingestion**, **asynchronous joins**, and **analytical loading** into ClickHouse.

---

## **Quality Dimensions Covered**

| Dimension | Risk |
| :---- | :---- |
| Synchronization & delay | Late events, CDC lag |
| Missing events | Dropped Kafka messages |
| Schema drift | Breaking changes |
| Load health | Backlogs, partial loads |
| Duplication | Reprocessing, retries |

---

## **1\. Synchronization & Delay Monitoring**

### **Problem**

Purchase events and order records arrive independently and may be delayed due to:

* CDC lag  
* Kafka backpressure  
* ClickHouse ingestion slowdown

**Metrics**

#### **Event Freshness**

`SELECT`  
    `max(event_time) AS latest_event_time,`  
    `now() - max(event_time) AS ingestion_lag`  
`FROM purchase_events_raw;`

#### **Enrichment Lag (Deferred Approach)**

`SELECT count()`  
`FROM purchase_events_raw p`  
`LEFT JOIN unified_orders o`  
`ON p.order_id = o.order_id`  
`WHERE o.order_id IS NULL;`

---

### **Alerts**

| Condition | Action |
| :---- | :---- |
| Ingestion lag \> SLA | Page on-call |
| Missing orders \> threshold | Investigate CDC delay |
| Growing backlog | Scale consumers |

---

## **2\. Missing Events Detection**

### **Problem**

Events may be lost due to:

* Kafka retention issues  
* Consumer crashes  
* Offset mismanagement

### **Controls**

#### **Count Reconciliation**

Compare event counts across stages:

`-- Kafka-ingested events`  
`SELECT count() FROM kafka_enriched_user_events;`

`-- Persisted raw events`  
`SELECT count() FROM purchase_events_raw;`

A significant delta indicates **data loss**.

---

### **Kafka-Level Monitoring**

* Consumer lag per partition  
* Rebalance frequency  
* Commit latency

---

### **Alert Example**

| Metric | Threshold |
| :---- | :---- |
| Kafka consumer lag | \> N messages |
| Offset stagnation | \> X minutes |

---

## **3\. Schema Drift Protection**

### **Problem**

Upstream schema changes can silently break:

* Materialized views  
* Aggregations  
* Downstream analytics

### **Preventive Measures**

#### **Schema Registry (Recommended)**

* Use **Avro \+ Schema Registry**  
* Enforce **BACKWARD compatibility**  
* Reject breaking changes

---

### **ClickHouse-Level Validation**

`SELECT *`  
`FROM system.errors`  
`WHERE name LIKE '%Kafka%'`

Monitor:

* Deserialization errors  
* Missing fields  
* Type mismatches

---

### **Governance Policy**

| Change Type | Allowed |
| :---- | :---- |
| Add optional field | ✅ |
| Rename field | ❌ |
| Change type | ❌ |
| Remove field | ❌ |

---

## **4\. Load & Ingestion Monitoring**

### **Problem**

ClickHouse is optimized for batch inserts.  
 Small or slow inserts degrade performance.

### **Metrics**

#### **Kafka Engine Health**

`SELECT`  
    `database,`  
    `table,`  
    `rows_read,`  
    `last_read_time`  
`FROM system.kafka_consumers;`

#### **Insert Load**

`SELECT`  
    `table,`  
    `sum(rows) AS rows_inserted`  
`FROM system.parts`  
`GROUP BY table;`  

### **Alerts**

| Condition | Response |
| :---- | :---- |
| No reads for X min | Restart consumer |
| Too many small parts | Tune batch size |
| Disk pressure | Increase TTL / partitions |

---

## **5\. Duplicate & Idempotency Checks**

### **Problem**

Reprocessing or retries can create duplicates.

### **Controls**

* Deterministic `purchase_id`  
* `ReplacingMergeTree(version)`  
* Periodic deduplication via merges

### **Validation Query**

`SELECT purchase_id, count()`  
`FROM purchase_event_facts`  
`GROUP BY purchase_id`  
`HAVING count() > 1;`

Duplicates should converge to one row after merges.

---

## **6\. Business-Level Data Quality Checks**

### **Examples**

#### **Financial Validations**

`SELECT count()`  
`FROM purchase_event_facts`  
`WHERE paid_amount > coverage_amount;`

#### **Mandatory Fields**

`SELECT count()`  
`FROM purchase_event_facts`  
`WHERE user_id IS NULL`  
   `OR order_id IS NULL;`

### **SLA-Based Rules**

| Rule | Severity |
| :---- | :---- |
| Null order\_id after 1 day | High |
| Negative premium | Critical |
| Unknown product type | Medium |

## **7\. Observability Stack**

### **Recommended Tools**

| Layer | Tool |
| :---- | :---- |
| Metrics | Prometheus |
| Dashboards | Grafana |
| Alerts | Alertmanager |
| Logs | Loki / ELK |
| Schema | Confluent Schema Registry |

---

## **Final Summary**

This data quality plan ensures:

* **No silent data loss**  
* **Controlled schema evolution**  
* **Clear freshness SLAs**  
* **Safe reprocessing**  
* **Auditable ingestion**

It balances **streaming reliability** with **warehouse-scale analytics**, and is designed to operate under real-world failure modes rather than ideal conditions.

## **Bonus – Backfill Strategy and Spark Backfill Job**

### **Motivation**

Despite strong streaming guarantees, missing or late data can still occur due to:

* Kafka retention limits  
* CDC outages  
* Schema evolution rollbacks  
* Logic bugs fixed after deployment

Therefore, the system must support **safe, repeatable backfills** without corrupting existing data.

## **Backfill Design Principles**

The backfill process follows these principles:

1. **Source of Truth**

   * Kafka (if retained) or OLTP databases (MySQL/Postgres)

2. **Idempotent Writes**

   * Same `purchase_id` logic as streaming

   * ClickHouse `ReplacingMergeTree(version)` ensures last-write-wins

3. **Isolation from Streaming**

   * Backfill runs as a **separate Spark batch job**

   * Does not interfere with Flink streaming pipeline

4. **Time-Bounded**

   * Backfill always runs for a **specific date range**

---

## **Backfill Scenarios Covered**

| Scenario | Solution |
| ----- | ----- |
| Missing Kafka data | Re-read from source DB |
| CDC downtime | Recompute orders |
| Bug fix in enrichment | Re-run join logic |
| Late-arriving data | Backfill by event\_date |

---

## 

## 

## **Backfill Architecture**

`Spark Batch Job`  
   `├── Read user events (Kafka / Parquet / DB)`  
   `├── Read users table (JDBC)`  
   `├── Read orders tables (JDBC)`  
   `├── Re-apply join & business logic`  
   `└── Write to ClickHouse (idempotent)`

---

## **Spark Backfill Job (Example)**

### **Assumptions**

* Backfill window: `[start_date, end_date)`  
* Spark runs in batch mode  
* Same schema and logic as Flink

---

### **1\. Read User Events**

`val userEvents = spark.read`  
  `.format("parquet") // or Kafka / JDBC`  
  `.load("/data/user_events/")`  
  `.filter(col("event_type") === "purchase")`  
  `.filter(col("event_date").between(startDate, endDate))`

---

### **2\. Read Users Table**

`val users = spark.read`  
  `.format("jdbc")`  
  `.option("url", "jdbc:mysql://mysql:3306/azki")`  
  `.option("dbtable", "users")`  
  `.option("user", "user")`  
  `.option("password", "password")`  
  `.load()`

### **3\. Read and Unify Orders**

`val third = spark.read.jdbc(mysqlUrl, "third_orders", props)`  
`val body = spark.read.jdbc(mysqlUrl, "body_orders", props)`  
`val medical = spark.read.jdbc(mysqlUrl, "medical_orders", props)`  
`val fire = spark.read.jdbc(mysqlUrl, "fire_orders", props)`  
`val financial = spark.read.jdbc(mysqlUrl, "financial_orders", props)`  
`val unifiedOrders =`  
  `third.unionByName(body)`  
       `.unionByName(medical)`  
       `.unionByName(fire)`  
       `.join(financial, "order_id")`  


### **4\. Enrich Purchase Events**

`val enrichedPurchases =`  
  `userEvents`  
    `.join(users, "user_id")`  
    `.join(unifiedOrders, Seq("order_id"), "left")`  
    `.withColumn(`  
      `"purchase_id",`  
      `expr("xxhash64(user_id, order_id, event_date)")`  
    `)`  
    `.withColumn(`  
      `"version",`  
      `unix_timestamp(col("event_time"))`  
    `)`  


### **5\. Write to ClickHouse (Idempotent)**

`enrichedPurchases.write`  
  `.format("jdbc")`  
  `.option("url", "jdbc:clickhouse://clickhouse:8123/azki")`  
  `.option("dbtable", "purchase_event_facts")`  
  `.option("batchsize", "100000")`  
  `.option("isolationLevel", "NONE")`  
  `.mode("append")`  
  `.save()`

## **Why This Is Safe**

* Duplicate rows collapse via `ReplacingMergeTree`  
* Backfilled data overwrites incorrect or missing rows  
* Streaming and batch share the **same primary key**  
* No deletes required

---

## **Operational Safeguards**

### **Pre-Backfill Checks**

* Pause Flink job (optional)

* Validate date range

* Snapshot ClickHouse partitions

### **Post-Backfill Validation**

`SELECT count()`  
`FROM purchase_event_facts`  
`WHERE event_date BETWEEN '2025-01-01' AND '2025-01-02';`

Compare with expected counts.

---

## **When Spark Is the Right Tool**

Spark is preferred for backfills because:

* Handles large historical scans efficiently  
* Supports complex joins  
* Easy to parallelize by date  
* Decoupled from streaming SLAs

## **Final Summary (Interview-Ready)**

“For missing data or logic fixes, we use a Spark batch backfill job that replays historical data, re-applies the same enrichment logic, and writes idempotently to ClickHouse using ReplacingMergeTree. This guarantees correctness without impacting the live streaming pipeline.”

