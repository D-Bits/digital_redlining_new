import pyspark.pipelines as dp


@dp.materialized_view(name="bronze_fcc.raw_mobile")
def raw_mobile():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/bronze_fcc/src_files/fcc_mobile_full_data.csv")

    return df

    
@dp.materialized_view(name="bronze_fcc.raw_fixed")
def raw_fixed():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/bronze_fcc/src_files/fcc_fixed_full_data.csv")

    return df
    

@dp.materialized_view(name="bronze_fcc.raw_provider")
def raw_provid():

    df = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/bronze_fcc/src_files/bdc_us_provider_list_D24_30sep2025.csv")

    return df
