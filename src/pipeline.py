from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StellarClassification") \
    .getOrCreate()  
    
spark.warnings.filter("ignore")  # Ignore warnings for cleaner output
spark.sparkContext.setLogLevel("ERROR") 
