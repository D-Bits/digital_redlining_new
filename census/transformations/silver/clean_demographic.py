from pyspark import pipelines as dp
from utilities.utils import silver_validations
import pyspark.sql.functions as F


@dp.expect_all_or_fail(silver_validations)
@dp.materialized_view(name="silver_census.clean_geo")
def cleaned_geo():

    raw_county = spark.read.table("digital_redlining.bronze_census.raw_demographic_county")
    raw_state = spark.read.table("digital_redlining.bronze_census.raw_demographic_state")
    raw_msa = spark.read.table("digital_redlining.bronze_census.raw_demographic_msa")
  
    # Add a geo_level column to each dataframe
    raw_county = raw_county.withColumn("geo_level", F.lit("county"))
    raw_state = raw_state.withColumn("geo_level", F.lit("state"))
    raw_msa = raw_msa.withColumn("geo_level", F.lit("msa"))
    # Drop unnecessary columns
    raw_county = raw_county.select(["GEO_ID", "NAME", "geo_level"])
    raw_state = raw_state.select(["GEO_ID", "NAME", "geo_level"])
    raw_msa = raw_msa.select(["GEO_ID", "NAME", "geo_level"])
    # Union the raw tables
    raw_combined = raw_county.union(raw_state).union(raw_msa)
    # Clean the data
    df = (
        raw_combined
        .withColumnRenamed("GEO_ID", "geo_id")
        .withColumnRenamed("NAME", "location_name")
    )
    # Drop the prefix from geoids
    df = df.withColumn("geo_id", F.expr("substring(geo_id, 8, length(geo_id)-7)"))
    # Add an "id" column
    df = df.withColumn("id", F.monotonically_increasing_id())
    df = df.select(['id', 'geo_id', 'location_name', 'geo_level'])

    return df


@dp.expect_all_or_fail({'no_null_geoids': 'geo_id IS NOT NULL'})
@dp.materialized_view(name="silver_census.clean_demographic")
def cleaned_demographic():

    raw_county = spark.read.table("digital_redlining.bronze_census.raw_demographic_county")
    raw_state = spark.read.table("digital_redlining.bronze_census.raw_demographic_state")
    raw_msa = spark.read.table("digital_redlining.bronze_census.raw_demographic_msa")

    # Drop unnecessary columns
    raw_county = raw_county.select(["GEO_ID", "P1_001N", "P1_003N", "P1_004N", "P1_005N", "P1_006N", "P1_007N", "P1_008N", "P1_009N", "P2_001N", "P2_002N"])
    raw_state = raw_state.select(["GEO_ID", "P1_001N", "P1_003N", "P1_004N", "P1_005N", "P1_006N", "P1_007N", "P1_008N", "P1_009N", "P2_001N", "P2_002N"])
    raw_msa = raw_msa.select(["GEO_ID", "P1_001N", "P1_003N", "P1_004N", "P1_005N", "P1_006N", "P1_007N", "P1_008N", "P1_009N", "P2_001N", "P2_002N"])
    
    # Union the county, state, and msa dataframes
    df = raw_county.union(raw_state).union(raw_msa)
    # Clean the data
    df = (
        df
        .withColumnRenamed("GEO_ID", "geo_id")
        .withColumnRenamed("P1_001N", "total_population")
        .withColumnRenamed("P1_003N", "male_population")
        .withColumnRenamed("P1_004N", "female_population")
        .withColumnRenamed("P1_005N", "white_population")
        .withColumnRenamed("P1_006N", "black_population")
        .withColumnRenamed("P1_007N", "asian_population")
        .withColumnRenamed("P1_008N", "hispanic_population")
        .withColumnRenamed("P1_009N", "other_population")
        .withColumnRenamed("P2_001N", "median_age")
    )
    df = df.withColumn("geo_id", F.expr("substring(geo_id, 8, length(geo_id)-7)"))
    df = df.withColumn("id", F.monotonically_increasing_id())

    # Type Cast columns
    df = df.withColumn("total_population", df.total_population.cast("int"))
    df = df.withColumn("male_population", df.male_population.cast("int"))
    df = df.withColumn("female_population", df.female_population.cast("int"))
    df = df.withColumn("white_population", df.white_population.cast("int"))
    df = df.withColumn("black_population", df.black_population.cast("int"))
    df = df.withColumn("asian_population", df.asian_population.cast("int"))
    df = df.withColumn("hispanic_population", df.hispanic_population.cast("int"))
    df = df.withColumn("other_population", df.other_population.cast("int"))
    df = df.withColumn("median_age", df.median_age.cast("int"))
    
    df = df.select(['id', 'geo_id', 'total_population', 'male_population', 'female_population', 'white_population', 'black_population', 'asian_population', 'hispanic_population', 'other_population', 'median_age'])
    
    return df


