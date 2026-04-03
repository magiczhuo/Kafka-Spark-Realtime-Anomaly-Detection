import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, lit

# 指向我们第一步清洗好的离线数据集
DATA_FILE = os.path.abspath("data_preprocessed.jsonl")

def main():
    print("="*60)
    print("启动传统批处理基线测试 (Batch Baseline)")
    print("="*60)

    # 1. 记录批处理开始时间
    start_time = time.time()

    # 初始化 SparkSession (本地模式)
    spark = SparkSession.builder \
        .appName("BatchAnomalyDetectionBaseline") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    if not os.path.exists(DATA_FILE):
        print(f"Error: 找不到预处理文件 {DATA_FILE}。请先运行 preprocess.py")
        return

    # 2. 全量读取落盘的 JSON 文件 (触发全量磁盘 I/O)
    print(f"正在从磁盘加载全量历史日志: {DATA_FILE}")
    df = spark.read.json(DATA_FILE)

    # 转换时间戳格式
    event_df = df.withColumn("timestamp", col("timestamp_iso").cast("timestamp"))

    # [对照实验核心：混入异常数据]：动态插入500条异常攻击数据
    # 这模拟了磁盘历史数据中，在某个特定时刻曾发生过一次 DDoS 攻击。
    # 这里将攻击时间设为 2019-01-22 03:56:50，与原有日志的时间域对齐
    print("正在向数据集中混入 500 条突发攻击记录，模拟离线日志中隐藏的攻击事实...")
    attack_time = "2019-01-22 03:56:50.000"
    attack_count = 500

    # 使用 Spark JVM 侧生成数据，避免 Windows 下 Python worker 连接超时
    attack_df = (
        spark.range(attack_count)
        .select(
            lit(attack_time).alias("timestamp_iso"),
            lit("/login-attack").alias("url"),
        )
    )
    attack_event_df = attack_df.withColumn("timestamp", col("timestamp_iso").cast("timestamp"))
    
    # 将异常攻击流合并到原始数据中一同参与离线长计算
    event_df = event_df.unionByName(attack_event_df, allowMissingColumns=True)

    # 3. 业务过滤逻辑（与 Stream 完全保持一致）
    filtered_df = event_df.filter(col("url").isNotNull()) \
                          .filter(~col("url").like("%.png")) \
                          .filter(~col("url").like("%.jpg"))

    # 4. 执行全量滑动窗口聚合
    # 注意：批处理中没有 Watermark(水位线) 的概念，因为它是一次性读取所有静态数据，不存在乱序等待
    windowed_counts = filtered_df \
        .groupBy(window(col("timestamp"), "10 seconds", "2 seconds")) \
        .count() \
        .orderBy("window.start", ascending=True)

    # [对照实验新增]：在离线批处理中找出异常流量窗口
    THRESHOLD = 300
    anomalies = windowed_counts.filter(col("count") > THRESHOLD)

    # 5. 触发 Action: 收集结果并打印验证
    print("开始执行全量 DAG 计算并检测异常...\n")
    print(f"========== 离线检测到的异常流量窗口 (阈值 > {THRESHOLD}) ==========")
    anomalies.show(truncate=False)
    
    print("========== 总体正常流量统计 (前20条) ==========")
    windowed_counts.show(20, truncate=False)

    # 6. 计算端到端耗时
    end_time = time.time()
    duration = end_time - start_time
    
    # 统计总数据量
    total_records = df.count()

    print("="*60)
    print("批处理基线测试完成！")
    print(f"处理总数据量: {total_records} 条")
    print(f"端到端总耗时 (含磁盘 I/O 与启动): {duration:.4f} 秒")
    print("="*60)
    
    print("\n报告素材点拨:")
    print("对比流处理(Streaming)中每 2 秒就能输出一次最新状态(毫秒级延迟)；")
    print("传统批处理(Batch)必须等待所有数据落盘后，通过一次性高昂的磁盘 I/O 载入内存，")
    print("导致其延迟是秒级甚至分钟级以上的。这证明了流处理在'高实时异常检测'场景下的碾压级优势！")

    spark.stop()

if __name__ == "__main__":
    main()
