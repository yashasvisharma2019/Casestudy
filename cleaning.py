# Databricks notebook source
client_id ="88dec17f-62f0-4cab-82b7-b9022c2cc21c"
client_secret="idG8Q~KkSdwEBRYgavLJ2DkzuYElGDbi2L3pIbjm"
directory_id="037e0179-cd0e-4201-951c-167b7554d77a"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# COMMAND ----------

mount_point = "/mnt/httpforstorage/raw"

if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Unmount the existing mount point
    dbutils.fs.unmount(mount_point)
    print(f"Unmounted existing mount at {mount_point}")

try:
    dbutils.fs.mount(
        source="abfss://raw@httpforstorage.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted successfully at{mount_point}")
except Exception as e:
    print(f"Error mounting: {e}")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/httpforstorage/raw"))


# COMMAND ----------

application_id ="88dec17f-62f0-4cab-82b7-b9022c2cc21c"
service_credential="idG8Q~KkSdwEBRYgavLJ2DkzuYElGDbi2L3pIbjm"
directory_id="037e0179-cd0e-4201-951c-167b7554d77a"
spark.conf.set("fs.azure.account.auth.type.httpforstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.httpforstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.httpforstorage.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.httpforstorage.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.httpforstorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@httpforstorage.dfs.core.windows.net"))

# COMMAND ----------

# Databricks Data Cleaning for COVID-19 Data
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, countDistinct
from pyspark.sql.types import StringType, IntegerType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("COVIDDataCleaning").getOrCreate()

# Load Patient Data
patient_file_path = "/mnt/httpforstorage/raw/ExpandedPatientData202412061539.csv"  # Update path if needed
patients_df = spark.read.format("csv").option("header", "true").load(patient_file_path)

# Load Test Data
test_file_path = "/mnt/httpforstorage/raw/ExpandedTestResults202412061540.csv"  # Update path if needed
tests_df = spark.read.format("csv").option("header", "true").load(test_file_path)

# --- Data Cleaning on Patients Data ---
# Drop rows with missing PatientID
patients_df = patients_df.filter(col("PatientID").isNotNull())

# Cast PatientID and Age to Integer and TestDate to Date
def clean_patient_data(df):
    return (df.withColumn("PatientID", col("PatientID").cast(IntegerType()))
              .withColumn("Age", col("Age").cast(IntegerType()))
              .withColumn("TestDate", col("TestDate").cast(DateType())))

patients_df = clean_patient_data(patients_df)


patients_df = patients_df.withColumn("COVIDStatus", when(col("COVIDStatus").isNull(), lit("Unknown"))
                                     .otherwise(col("COVIDStatus")))


tests_df = tests_df.filter(col("TestID").isNotNull() & col("PatientID").isNotNull())


def clean_test_data(df):
    return (df.withColumn("TestID", col("TestID").cast(IntegerType()))
              .withColumn("PatientID", col("PatientID").cast(IntegerType()))
              .withColumn("ResultDate", col("ResultDate").cast(DateType())))

tests_df = clean_test_data(tests_df)


tests_df = tests_df.withColumn("LabName", when(col("LabName").isNull(), lit("Unknown"))
                               .otherwise(col("LabName")))

patients_df.select(countDistinct("PatientID").alias("Unique Patients")).show()
tests_df.select(countDistinct("TestID").alias("Unique Tests")).show()

# Save cleaned data as Parquet files and use Delta Lake for appending
cleaned_patient_file_path = "/mnt/httpforstorage/processed/CleanedPatientData"
cleaned_test_file_path = "/mnt/httpforstorage/processed/CleanedTestResults"
cleaned_patient_output_path = "abfss://processed@httpforstorage.dfs.core.windows.net/CleanedPatientData"
cleaned_test_output_path = "abfss://processed@httpforstorage.dfs.core.windows.net/CleanedTestResults"

patients_df.write.format("delta").mode("append").save(cleaned_patient_file_path)
tests_df.write.format("delta").mode("append").save(cleaned_test_file_path)
patients_df.write.format("delta").mode("overwrite").save(cleaned_patient_output_path)
print(f"Cleaned patient data saved to {cleaned_patient_output_path}")

tests_df.write.format("delta").mode("append").save(cleaned_test_output_path)
print(f"Cleaned test data saved to {cleaned_test_output_path}")

print(f"Cleaned data saved to {cleaned_patient_file_path} and {cleaned_test_file_path} using Delta Lake")
tests_df.show()
patients_df.show()


# COMMAND ----------



patients_df = spark.read.format("delta").load(cleaned_patient_output_path)
tests_df = spark.read.format("delta").load(cleaned_test_output_path)

# Join the cleaned patient and test data on PatientID
joined_df = patients_df.join(tests_df, on="PatientID", how="inner")

# Add a new column to indicate if a patient's test result is 'Positive' or 'Negative'
joined_df = joined_df.withColumn("ResultStatus", when(col("COVIDStatus") == "Positive", lit("Positive"))
                                               .otherwise(lit("Negative")))

# Select and transform relevant columns for reporting
etl_output_df = joined_df.select(
    col("PatientID"),
    col("Age"),
    col("COVIDStatus"),
    col("TestID"),
    col("LabName"),
    col("ResultDate"),
    col("ResultStatus")
)

# Show a preview of the transformed data
etl_output_df.show()

# Save the transformed data to Delta format in Azure Storage
etl_output_path = "abfss://gold@httpforstorage.dfs.core.windows.net/ETLResultData"

etl_output_df.write.format("delta").mode("append").save(etl_output_path)
print(f"ETL output data saved to {etl_output_path}")

# COMMAND ----------

print(patients_df.columns)
