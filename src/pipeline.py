import time
import warnings

from pyspark.ml import Pipeline
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import RandomForestClassifier

warnings.filterwarnings("ignore")


def create_spark():
    
    spark = (SparkSession.builder
             .appName("StellarClassification")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
               
def load_data(spark):
    #base = "file:///Users/tsampikakiaourtzi/projects/big_data_project/data/"     #for Mika
                             
    base = "file:///opt/spark/work-dir/data/"

    p1 = spark.read.csv(f"{base}master", header=True, inferSchema=True)
    p2 = spark.read.csv(f"{base}worker1", header=True, inferSchema=True)
    p3 = spark.read.csv(f"{base}worker2", header=True, inferSchema=True)

    return p1.unionByName(p2).unionByName(p3)


def preprocess(dataset, spark):                                
    df = dataset
    df.createOrReplaceTempView("data")

    # -9999 values in u, g and z that are placeholders for bad measurements
    df_filtered = spark.sql(f"""
        SELECT u, g, r, i, z, redshift, class,
        u - g AS u_g, g - r AS g_r, r - i AS r_i, i - z AS i_z  
        FROM data
        WHERE u > -100 AND g > -100 AND z > -100""")  # noqa: F541

    df_filtered = StringIndexer(
    inputCol='class',
    outputCol='type',
    handleInvalid='keep').fit(df_filtered).transform(df_filtered)

    df_filtered = df_filtered.drop('class')

    required_feature = ['u', 'g', 'r', 'i', 'z', 'redshift', 'u_g', 'g_r', 'r_i', 'i_z']
    assembler = VectorAssembler(inputCols=required_feature, outputCol='features')
    transformed_data = assembler.transform(df_filtered)

    # Add an ID as help for later
    transformed_data_id = transformed_data.withColumn("row_id", F.monotonically_increasing_id())

    # Fractions per label (stratified sampling)
    train_fractions = {
        0.0 : 0.8,
        1.0 : 0.8,
        2.0 : 0.8
    }

    # Sample training set
    train_df = transformed_data_id.sampleBy('type', fractions=train_fractions, seed=42)
    # Get remaining rows for the test set (0.2 per class)
    test_df = transformed_data_id.join(train_df, on="row_id", how='left_anti')

    train_df = train_df.drop('row_id')
    test_df = test_df.drop('row_id')

    return train_df, test_df

def random_forest_classifier(train): 
    train_df = train
    rf = RandomForestClassifier(labelCol="type", featuresCol="features")
    model = rf.fit(train_df)
    return model


#def logistic_regression_classifier(train, test):        # Mika
#def random_forest_classifier(train, test):              # Ieva

def mlp_classifier(train, test):                         # Mika
   
    
    n_classes = train.union(test).select("type").distinct().count()

    scaler = StandardScaler(inputCol="features", outputCol="scaled", withMean=True, withStd=True)

  
    mlp = MultilayerPerceptronClassifier(featuresCol="scaled", labelCol="type", layers=[6, 16, 8, n_classes],seed=42)

    pipeline = Pipeline(stages=[scaler, mlp])

    # keep training time
    start = time.time()
    model = pipeline.fit(train)
    train_time = time.time() - start

    # predictions
    preds = model.transform(test)

    # evaluation metrics
    metrics = {}
    for metric in ["accuracy", "f1"]:
        evaluator = MulticlassClassificationEvaluator(labelCol="type", predictionCol="prediction", metricName=metric)
        metrics[metric] = evaluator.evaluate(preds)

    result = {"model": "MLP (baseline)","accuracy": metrics["accuracy"],"f1": metrics["f1"],"train_time": train_time}

    print(f"Number of classes: {n_classes}")
    print(f"\n{'Model':<18}{'Accuracy':>10}{'F1':>10}{'Train (s)':>12}")
    print(f"{result['model']:<18}{result['accuracy']:>10.4f}{result['f1']:>10.4f}{result['train_time']:>12.1f}")

    # confusion matrix 
    preds.groupBy("type").pivot("prediction").count().orderBy("type").show() # rows = true class, columns = predicted class

    return model, preds, result
    
    
#def evaluate(): 
    
def main(): 
    spark = create_spark()
    df = load_data(spark)
    train_df, test_df = preprocess(df, spark)
    rf_model = random_forest_classifier(train_df)

    spark.stop()
    
if __name__ == "__main__":
    main()