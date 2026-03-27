from utilities.utils import silver_validations
import pyspark.sql.functions as F
import pyspark.pipelines as dp


@dp.materialized_view(name="silver_census.clean_income")
@dp.expect_all_or_fail(silver_validations)
def cleaned_income():

    raw_county = spark.read.table("digital_redlining.bronze_census.raw_income_county")
    raw_state = spark.read.table("digital_redlining.bronze_census.raw_income_state")
    raw_msa = spark.read.table("digital_redlining.bronze_census.raw_income_msa")

    # Union dataframes
    df = raw_county.union(raw_msa).union(raw_state)
    # Rename columns
    df = df.withColumnRenamed("GEO_ID", "geo_id")
    df = df.withColumnRenamed('S1903_C01_002E', 'white_median_income')
    df = df.withColumnRenamed('S1903_C01_003E', 'black_median_income')
    df = df.withColumnRenamed('S1903_C01_004E', 'native_median_income')
    df = df.withColumnRenamed('S1903_C01_005E', 'asian_median_income')
    df = df.withColumnRenamed('S1903_C01_006E', 'pac_islander_median_income')
    df = df.withColumnRenamed('S1903_C01_007E', 'other_median_income')
    df = df.withColumnRenamed('S1903_C01_009E', 'hispanic_median_income')
    # Type cast columns
    df = df.withColumn("white_median_income", F.expr("cast(white_median_income as double)"))
    df = df.withColumn("black_median_income", F.expr("cast(black_median_income as double)"))
    df = df.withColumn("native_median_income", F.expr("cast(native_median_income as double)"))
    df = df.withColumn("asian_median_income", F.expr("cast(asian_median_income as double)"))
    df = df.withColumn("pac_islander_median_income", F.expr("cast(pac_islander_median_income as double)"))
    df = df.withColumn("other_median_income", F.expr("cast(other_median_income as double)"))
    df = df.withColumn("hispanic_median_income", F.expr("cast(hispanic_median_income as double)"))
    
    # Drop the prefix from geoids
    df = df.withColumn("geo_id", F.expr("substring(geo_id, 10, length(geo_id))"))
    # Depublicate on geo_id
    df = df.drop_duplicates(["geo_id"])
    df = df.withColumn("id", F.monotonically_increasing_id())
    # Drop unnecessary columns
    df = df.select([
        "geo_id", 
        "white_median_income", 
        "black_median_income",
        "native_median_income", 
        "asian_median_income", 
        "pac_islander_median_income", 
        "other_median_income", 
        "hispanic_median_income"
    ])


    return df
