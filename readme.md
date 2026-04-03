# Real-Time Anomaly Detection Pipeline

This project demonstrates a real-time network traffic anomaly detection system leveraging PySpark Structured Streaming, Apache Kafka, and Streamlit. The pipeline ingests continuous web logs, computes traffic via sliding windows with watermarks, and triggers low-latency front-end alerts against DDoS-like burst access.

## 🏗️ Architecture & Workflow

1. **Data Preprocessing**: Raw server logs (e.g., Access_10000.log) are cleaned and standardized into structured JSON formats (data_preprocessed.jsonl) for efficient consumption.
2. **Kafka Ingestion (producer.py)**: Acts as the real-time event source. It continuously replays historical logs into the Kafka web-logs topic. Furthermore, it supports manual injections of burst traffic (e.g., 500 requests sharing an identical timestamp) to reliably simulate sudden DDoS attacks.
3. **Stream Processing (spark_processor.py)**:
   - Subscribes to the live Kafka stream.
   - Extracts absolute event-time from JSON payloads.
   - **Sliding Window:** Groups traffic into 10-second windows shifting every 2 seconds (window(col(''timestamp''), ''10 seconds'', ''2 seconds'')).
   - **Watermark Protection:** Uses a 30-second watermark (WATERMARK_DELAY = ''30 seconds'') to gracefully accommodate network delays without dropping late-arriving events prematurely.
   - **Sink Persistence:** Saves the micro-batch aggregated states cross-process safely into a local SQLite database (metrics.db) via the native foreachBatch operation.
4. **Real-Time Dashboard (dashboard.py)**:
   - A reactive Streamlit application polling SQLite dynamically.
   - Instead of checking only the volatile active micro-batch, it evaluates anomaly thresholds by reviewing the peak volume uniformly across the **last 3 valid sliding windows** (ALERT_LOOKBACK_WINDOWS = 3). 
   - If traffic exceeds the threshold (THRESHOLD = 300), the dashboard immediately turns red and raises a critical alarm.

## ⚙️ Environment Setup (Windows Version)

Ensure the following dependencies are installed and correctly added to your system environment variables (especially for Windows):
- **Python 3.11** (Managed via Conda, environment name: `base` or custom)
- **Java 17** (`JAVA_HOME` configured)
- **Hadoop Winutils** (`HADOOP_HOME` configured for PySpark on Windows)
- **Apache Kafka & Zookeeper** (Running locally on `localhost:9092`)
- Required Python Packages: `pyspark`, `kafka-python`, `streamlit`, `plotly`, `pandas`; Don't worry about the version conflict between pyspark and kafka, our code can automatically generate compatible Kafka connector coordinates based on the current PySpark version

---
## 🔬 Baseline vs. Streaming (Experimental Evaluation)

To strictly measure and quantify the architectural improvement, we designed a **Batch Processing Baseline (batch_baseline.py)** reading strictly from disk (acting as HDFS/Hadoop storage). Below details the comparative scheme for both methodologies.

### The Batch Baseline Scheme:
- **Operation Format**: Reads the complete static JSON dataset (data_preprocessed.jsonl) entirely utilizing raw disk I/O.
- **Anomaly Detection**: After mapping the data via .groupBy().count(), the system runs a .filter(col(''count'') > THRESHOLD) operation (e.g., >300) traversing the computed memory block to report any anomalous windows retroactively.

### Direct Comparison & Streaming Advantages:
1. **Detection Timeliness (Latency)**:
   - *Batch Baseline*: The entire data chunk must accumulate, settle physically on disk, and undergo heavy map-reduce scanning. Anomalies are exposed strictly **Post-Mortem** (latency in minutes to hours).
   - *Optimized Streaming (Kafka + Spark)*: Events calculate incrementally directly in memory (State Store). The anomaly triggers the Streamlit GUI dynamically within **Seconds** of the attack occurring.
2. **Resource Footprint & I/O Overhead**:
   - *Batch Baseline*: Scaling up the application implies mounting heavier data files sequentially, requiring tremendous parallel memory bounds creating high risks for JVM OutOfMemoryErrors.
   - *Optimized Streaming (Kafka + Spark)*: Streams compute entirely locally avoiding extreme payload overheads. PySpark purges expired window aggregations once they cross the *30-second Watermark*, guaranteeing a **stable computing footprint indefinitely.**
3. **Resilience to Late Data**:
   - *Batch Baseline*: Trivializes the concept of late data logic, since all data is historically sorted during computation.
   - *Optimized Streaming (Kafka + Spark)*: Models chaos by applying the 30-second Watermark, providing immense robustness handling un-sequenced logs due to arbitrary network topologies exactly dynamically.

---
## 🚀 How to Execute the Pipeline

Executing raw .sh Linux scripts directly inside Windows triggers the unconfigured WSL module leading to No such file or directory faults. To seamlessly execute the multi-tiered architecture, we provide automated PowerShell orchestration scripts.

> **IMPORTANT:** Confirm Zookeeper and Kafka are natively booted prior to launching!

### Mode 1: Execute the Optimized Real-Time Pipeline
`powershell
.\test.ps1
`
*(The script will autonomously launch 3 individual PowerShell terminals, activating the Conda environment and running the respective Spark, Producer, and Dashboard instances safely.)*

### Mode 2: Verify against the Initial Benchmark
`powershell
.\test_init.ps1
`

## ⏱️ Manual Reproduction & Simulating Attacks

Once 	est.ps1 runs:
1. Observe the live charts floating natively inside your launched browser stream (http://localhost:8501).
2. Switch back to the newly executed `Kafka Producer` terminal dedicated for producer.py. 
3. **Press ''Enter''** on your keyboard.
4. The terminal confirms the deployment via [ATTACK] Injected 500 burst events.
5. Check your Dashboard—an abnormal visual peak will violently spike above the threshold line rendering the application background aggressively indicating real-time identification.
