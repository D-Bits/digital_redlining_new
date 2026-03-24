import pyspark.pipelines as dp


# @dp.materialized_view(name="silver_census.clean_income")
# def cleaned_income():

#     raw_county = spark.read.table("digital_redlining.bronze_census.raw_income_county")
#     raw_state = spark.read.table("digital_redlining.bronze_census.raw_income_state")
#     raw_msa = spark.read.table("digital_redlining.bronze_census.raw_income_msa")

#     # Rename columns