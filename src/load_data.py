from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, when, count
from pyspark.ml.feature import StringIndexer, VectorAssembler

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

# Looking at sizes of datasets
#print(f"Dimension of Master Dataframe: {p1.count(), len(p1.columns)}")
#print(f"Dimension of Worker 1 Dataframe: {p2.count(), len(p2.columns)}")
#print(f"Dimension of Worker 2 Dataframe: {p3.count(), len(p3.columns)}")
#print(f"Dimension of Combined Dataframe: {df.count(), len(df.columns)}")

# Spread work across the 6 total cores used
#print("Partitions before repartition:", df.rdd.getNumPartitions())
df = df.repartition(6).cache()
#print("Partitions after repartition: ", df.rdd.getNumPartitions())

#df.printSchema()

df.createOrReplaceTempView("data")

df_filtered = spark.sql("SELECT u, g, r, i, z, redshift, class FROM data")
df_filtered.createOrReplaceTempView("filtered")
spark.sql("SELECT * FROM filtered WHERE u < -100 OR g < -100 OR z < -100").show()

#class_counts = spark.sql("SELECT class, COUNT(class) FROM filtered GROUP BY class")
#class_counts.show()

# Show null values
#df_filtered.select([count(when(isnull(c), c)).alias(c) for c in df_filtered.columns]).show()

df_filtered = StringIndexer(
    inputCol='class',
    outputCol='type',
    handleInvalid='keep').fit(df_filtered).transform(df_filtered)

# Dropping old values
df_filtered = df_filtered.drop('class')

required_feature = ['u', 'g', 'r', 'i', 'z', 'redshift']
assembler = VectorAssembler(inputCols=required_feature, outputCol='features')
transformed_data = assembler.transform(df_filtered)

spark.stop()