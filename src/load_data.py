from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("galaxy-load-dataset")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:/opt/spark/events")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "file:///opt/spark/work-dir/data/"

p1 = spark.read.csv(f"{base}master", header=True, inferSchema=True)
p2 = spark.read.csv(f"{base}worker1", header=True, inferSchema=True)
p3 = spark.read.csv(f"{base}worker2", header=True, inferSchema=True)

df = p1.unionByName(p2).unionByName(p3)


#print(f"Dimension of Master Dataframe: {p1.count(), len(p1.columns)}")
#print(f"Dimension of Worker 1 Dataframe: {p2.count(), len(p2.columns)}")
#print(f"Dimension of Worker 2 Dataframe: {p3.count(), len(p3.columns)}")
#print(f"Dimension of Combined Dataframe: {df.count(), len(df.columns)}")

# Spread work across the 6 total cores used
#print("Partitions before repartition:", df.rdd.getNumPartitions())
df = df.repartition(6).cache()
#print("Partitions after repartition: ", df.rdd.getNumPartitions())


spark.stop()