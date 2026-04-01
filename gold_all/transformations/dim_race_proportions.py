from pyspark import pipelines as dp


@dp.materialized_view(name="gold_all.dim_race_proportions")
def dim_race_proportions():

    demo = spark.read.table("digital_redlining.silver_census.clean_demographic")
    geo = spark.read.table("digital_redlining.gold_all.fact_geo")

    # Join the two tables
    df = demo.join(geo, on="geo_id", how="inner")
    # Find racial proportions
    df = df.withColumn("white_prop", df.white_population / df.total_population)
    df = df.withColumn("black_prop", df.black_population / df.total_population)
    df = df.withColumn("asian_prop", df.asian_population / df.total_population)
    df = df.withColumn("hispanic_prop", df.hispanic_population / df.total_population)
    df = df.withColumn("pac_islander_prop", df.pac_islander_population / df.total_population)
    df = df.withColumn("native_prop", df.native_population / df.total_population)
    df = df.withColumn("other_prop", df.other_population / df.total_population)
    df = df.withColumn("two_or_more_races_prop", df.two_or_more_races / df.total_population)

    df = df.select([
        "geo_id", 
        "white_prop", 
        "black_prop", 
        "asian_prop", 
        "hispanic_prop", 
        "pac_islander_prop", 
        "native_prop", 
        "other_prop", 
        "two_or_more_races_prop"
    ])

    return df
