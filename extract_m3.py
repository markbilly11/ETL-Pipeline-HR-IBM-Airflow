from pyspark.sql import SparkSession
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("EcommerceDW") \
    .getOrCreate()

# load dataset HR
df = spark.read.csv('/opt/airflow/data/HR_IBM_Dataset.csv',header=True)
# simpan hasil extract
df.write.csv('/opt/airflow/extract_result_hr_pipeline.csv', mode='overwrite')