from pyspark import pipelines as dp


def fcc_geo_ids():

    fixed = spark.read.table("fixed_fcc.fact_geo")
