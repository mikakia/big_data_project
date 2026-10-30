from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("galaxy-load-dataset")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "file:///opt/spark/work-dir/data/"

p1 = spark.read.csv(f"{base}master", header=True, inferSchema=True)
p2 = spark.read.csv(f"{base}worker1", header=True, inferSchema=True)
p3 = spark.read.csv(f"{base}worker2", header=True, inferSchema=True)

df = p1.unionByName(p2).unionByName(p3)

df.show(5)

spark.stop()