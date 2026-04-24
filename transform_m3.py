from pyspark.sql.functions import concat_ws, col, current_timestamp
from pyspark.sql.functions import col, when, count, sum
from pyspark.sql.types import IntegerType

from pyspark.sql import SparkSession
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("EcommerceDW") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

df = spark.read.csv('/opt/airflow/data/HR_IBM_Dataset.csv',header=True)

# DATA CLEANING
df = df.drop(
    'EmployeeCount',
    'StandardHours',
    'Over18'
)

# FEATURE ENGINEERING
df = df.withColumn(
    'employee_profile_id',
    concat_ws('_',
        col('EmployeeNumber'),
        col('Department'),
        col('JobRole')
    )
)

# TAMBAHAN (setara datetime.now())
df = df.withColumn(
    'datetime',
    current_timestamp()
)

# Great Expectation
validation_results = {}

# 1. UNIQUE
validation_results["unique"] = (
    df.groupBy("employee_profile_id")
      .count()
      .filter(col("count") > 1)
      .count() == 0
)

# 2. AGE BETWEEN
validation_results["age_between"] = (
    df.filter((col("Age") < 18) | (col("Age") > 60)).count() == 0
)

# 3. ATTRITION IN SET
validation_results["attrition_valid"] = (
    df.filter(~col("Attrition").isin("Yes", "No")).count() == 0
)

# 4. TYPE CHECK
validation_results["monthlyincome_type"] = (
    df.filter(col("MonthlyIncome").cast(IntegerType()).isNull()).count() == 0
)

# 5. NOT NULL
validation_results["jobrole_not_null"] = (
    df.filter(col("JobRole").isNull()).count() == 0
)

# 6. GENDER VALID
validation_results["gender_valid"] = (
    df.select("Gender").distinct()
      .filter(~col("Gender").isin("Male", "Female"))
      .count() == 0
)

# 7. DEPARTMENT VALID
validation_results["department_valid"] = (
    df.select("Department").distinct()
      .filter(~col("Department").isin(
          "Sales",
          "Research & Development",
          "Human Resources"
      )).count() == 0
)

# SAVE HASIL TRANSFORM
df.write.csv('/opt/airflow/data/extract_result_hr_pipeline/Hr_IBM.csv', mode='overwrite',header = True)