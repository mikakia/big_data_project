from email.mime import base
import warnings
from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")


def create_spark():
    
    spark = (SparkSession.builder
             .appName("StellarClassification")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
               
def load_data(spark):                             # Ieva 
    base = "file:///opt/spark/work-dir/data/"

    p1 = spark.read.csv(f"{base}master", header=True, inferSchema=True)
    p2 = spark.read.csv(f"{base}worker1", header=True, inferSchema=True)
    p3 = spark.read.csv(f"{base}worker2", header=True, inferSchema=True)

    return p1.unionByName(p2).unionByName(p3)


#def preprocess(dataset):                                # Ieva

#def random_forest_classifier(train, test):              # Ieva
#def logistic_regression_classifier(train, test):        # Mika
#def evaluate(): 
    
def main(): 
    spark = create_spark()
    df = load_data(spark)
    df.show(5)
    spark.stop()
    
if __name__ == "__main__":
    main()