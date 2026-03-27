from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(name="silver_fcc.clean_mobile")
def clean_mobile():

    mobile = spark.read.table("digital_redlining.bronze_fcc.raw_mobile")

    # Renamed to geography_id field for compatibility
    mobile = mobile.withColumnRenamed("geography_id", "geo_id")
    # Add id field 
    mobile = mobile.withColumn("id", F.monotonically_increasing_id())
    # Type cast columns
    mobile = mobile.withColumn("total_area", F.col("total_area").cast("float"))
    mobile = mobile.withColumn("mobilebb_3g_area_st_pct", F.col("mobilebb_3g_area_st_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_3g_area_iv_pct", F.col("mobilebb_3g_area_iv_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_4g_area_st_pct", F.col("mobilebb_4g_area_st_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_4g_area_st_pct", F.col("mobilebb_4g_area_st_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_4g_area_iv_pct", F.col("mobilebb_4g_area_iv_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_5g_spd1_area_st_pct", F.col("mobilebb_5g_spd1_area_st_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_5g_spd1_area_iv_pct", F.col("mobilebb_5g_spd1_area_iv_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_5g_spd2_area_st_pct", F.col("mobilebb_5g_spd2_area_st_pct").cast("float"))
    mobile = mobile.withColumn("mobilebb_5g_spd2_area_iv_pct", F.col("mobilebb_5g_spd2_area_iv_pct").cast("float"))
    # Drop unnecessary columns
    df = mobile.select([
        'id', 
        'geo_id',
        'total_area',
        'mobilebb_3g_area_st_pct',
        'mobilebb_3g_area_iv_pct',
        'mobilebb_4g_area_st_pct',
        'mobilebb_4g_area_iv_pct',
        'mobilebb_5g_spd1_area_st_pct',
        'mobilebb_5g_spd1_area_iv_pct',
        'mobilebb_5g_spd2_area_st_pct',
        'mobilebb_5g_spd2_area_iv_pct'
    ])

    return df
