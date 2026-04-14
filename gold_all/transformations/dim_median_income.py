import pyspark.pipelines as dp


@dp.materialized_view(name="gold_all.dim_income")
def dim_income():

    df = spark.table.read("digital_redlining.silver_census.clean_income")

    return df
