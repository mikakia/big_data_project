from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StellarClassification") \
    .getOrCreate()  
    
spark.warnings.filter("ignore")  # Ignore warnings for cleaner output
spark.sparkContext.setLogLevel("ERROR") 

def create_spark(master):            
def load_data(spark, mode):        
def preprocess(dataset):   

def random_forest_classifier(train, test):              # Ieva
def logistic_regression_classifier(train, test):        # Mika
def evaluate(): 
    
def main(): 