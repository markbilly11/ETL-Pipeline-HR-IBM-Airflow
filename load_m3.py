from pyspark.sql import SparkSession
from pymongo import MongoClient

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HR_IBMDW") \
    .getOrCreate()

client = MongoClient("mongodb+srv://mark:mark@cluster0.4godwvc.mongodb.net/")
df = spark.read.csv('/opt/airflow/data/extract_result_hr_pipeline/Hr_IBM.csv',header = True)
df = df.toPandas()

document_list = df.to_dict(orient='records')
document_list

client['sample_HR_IBM']['Dataset'].insert_many(document_list)