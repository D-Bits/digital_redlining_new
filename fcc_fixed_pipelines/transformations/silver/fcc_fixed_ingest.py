from pyspark import pipelines as dp
from pyspark.sql.functions import col


df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/src_files/fcc/fcc_fixed_full_data.csv")

# @dp.materialized_view
# def extract_fcc_fixed_src():

#     return spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/src_files/fcc/fcc_fixed_full_data.csv")


@dp.materialized_view(
    name="fcc_fixed.fact_geo",
    schema="fcc_fixed",
    refresh_policy="incremental"
)
def clean_fact_geo():

    fact_geo = df.select(['geography_id', 'area_data_type', 'geography_type', 'geography_desc',])

    return fact_geo

@dp.materialized_view(
    name="fcc_fixed.dim_tech",
    schema="fcc_fixed",
    refresh_policy="incremental"
)
def clean_dim_tech():

    dim_tech = df.select(['geography_id', 'area_data_type', 'biz_res', 'technology', 'total_units',])
    dim_tech = dim_tech.withColumn('total_units', col('total_units').cast('int'))

    return dim_tech

@dp.materialized_view(
    name="fcc_fixed.dim_speed",
    schema="fcc_fixed",
    refresh_policy="incremental"
)
def clean_dim_speed():

    dim_speed = df.select(['geography_id', 'speed_02_02', 'speed_10_1', 'speed_25_3', 'speed_100_20', 'speed_250_25', 'speed_1000_100',])

    speed_cols = [
        'speed_02_02',
        'speed_10_1',
        'speed_25_3',
        'speed_100_20',
        'speed_250_25',
        'speed_1000_100'
    ]

    # Type cast columns for dim_speed
    for col_name in speed_cols:
        dim_speed = dim_speed.withColumn(col_name, col(col_name).cast('double'))

    return dim_speed


# raw_src = extract_fcc_fixed_src()
fact_geo = clean_fact_geo()
dim_tech = clean_dim_tech()
dim_speed = clean_dim_speed()

# Write the cleaned data to sink tables
# fact_geo.write.mode("overwrite").saveAsTable("digital_redlining.fcc_fixed.fact_geo")
# dim_speed = dim_speed.withColumn("speed_02_02", col("speed_02_02").cast('double'))
# dim_speed.write.mode("overwrite").saveAsTable("digital_redlining.fcc_fixed.dim_speed")
# dim_tech.write.mode("overwrite").saveAsTable("digital_redlining.fcc_fixed.dim_tech")
