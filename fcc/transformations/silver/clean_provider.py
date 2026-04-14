from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(name="silver_fcc.clean_provider")
def clean_provider():

    df = spark.read.table("digital_redlining.bronze_fcc.raw_provider")
    # Type casting 
    df = df.withColumn("provider_id", F.col("provider_id").cast("int"))
    # Add id column 
    df = df.withColumn("id", F.monotonically_increasing_id())
    df = df.select(["id", "provider_id", "frn", "holding_company"])

    return df
