from pyspark.sql import DataFrame
from pyspark.sql import functions a f

partition_cols = spark.sql(f'describe detail delta.`{path}`').select('partitionColumns').collect()[0][0]

JDeltaLog = spark._jvm.org.apache.spark.sql.delta.DeltaLog
all_files_jdf = JDeltaLog.forTable(spark._jsparkSession, path).snapshot().allFiles().toDF()
all_files_df = DataFrame(all_files_jdf, spark._wrapped)
partition_counts_df = (
    all_files_df
    .groupBy([f.col('partitionValues')[key].alias(key) for key in partition_cols])
    .count()
)