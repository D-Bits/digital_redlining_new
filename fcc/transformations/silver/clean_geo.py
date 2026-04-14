from pyspark import pipelines as dp


# Clean the raw data for the FCC geography table
@dp.materialized_view(name="silver_fcc.clean_geo")
def geo_clean():

    fixed = spark.read.table("digital_redlining.bronze_fcc.raw_fixed")
    mobile = spark.read.table("digital_redlining.bronze_fcc.raw_mobile")

    # Rename columns to avoid duplicate names
    fixed = fixed.withColumnRenamed("area_data_type", "area_data_type_fixed")
    fixed = fixed.withColumnRenamed("geography_type", "geography_type_fixed")
    fixed = fixed.withColumnRenamed("geography_desc", "geography_desc_fixed")
    # fixed = fixed.withColumnRenamed("geography_desc_full", "geography_desc_full_fixed")

    # Inner join the fixed and mobile data using geo_ids
    df = fixed.join(mobile, on="geography_id", how="inner")
    df = df.select([
        "geography_id", 
        "area_data_type", 
        "geography_type", 
        "geography_desc"
    ])
    df = df.withColumnRenamed("geography_id", "geo_id")
    # De-duplicate on geo_id
    df = df.dropDuplicates(["geo_id"])
    
    return df
