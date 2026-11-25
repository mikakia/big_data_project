import warnings
from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")


def create_spark():
    
    spark = (SparkSession.builder
             .appName("StellarClassification")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
               
#def load_data(spark, mode):                             # Ieva 
#def preprocess(dataset):                                # Ieva

#def random_forest_classifier(train, test):              # Ieva
#def logistic_regression_classifier(train, test):        # Mika
#def evaluate(): 
    
def main(): 
    spark = create_spark()
    spark.stop()
    
if __name__ == "__main__":
    main()