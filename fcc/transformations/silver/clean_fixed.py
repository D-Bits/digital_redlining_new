from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(name="silver_fcc.clean_fixed")
def clean_fixed():

    fixed = spark.read.table("digital_redlining.bronze_fcc.raw_fixed")
    # Renamed to geography_id field for compatibility
    fixed = fixed.withColumnRenamed("geography_id", "geo_id")
    # Add id field 
    fixed = fixed.withColumn("id", F.monotonically_increasing_id())
    # Type cast columns
    fixed = fixed.withColumn("total_units", F.col("total_units").cast("int"))
    fixed = fixed.withColumn("speed_02_02", F.col("speed_02_02").cast("float"))
    fixed = fixed.withColumn("speed_10_1", F.col("speed_10_1").cast("float"))
    fixed = fixed.withColumn("speed_25_3", F.col("speed_25_3").cast("float"))
    fixed = fixed.withColumn("speed_100_20", F.col("speed_100_20").cast("float"))
    fixed = fixed.withColumn("speed_250_25", F.col("speed_250_25").cast("float"))
    fixed = fixed.withColumn("speed_1000_100", F.col("speed_1000_100").cast("float"))
    # Drop unnecessary columns
    df = fixed.select([
        'id', 
        'geo_id',
        'total_units',
        'biz_res',
        'technology',
        'speed_02_02',
        'speed_10_1',
        'speed_25_3',
        'speed_100_20',
        'speed_250_25',
        'speed_1000_100'
    ])
    
    return df

