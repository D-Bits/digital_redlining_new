from pyspark import pipelines as dp


@dp.materialized_view(name="demographic.raw_combined")
def raw_combined():

    # Load source files into memory
    msa = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/demographic/src_files/msa/ACSDP5Y2024.DP05-Data.csv")
    congressional = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/demographic/src_files/congressional/ACSDP5Y2024.DP05-Data.csv")
    county = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/demographic/src_files/county/ACSDP5Y2024.DP05-Data.csv")
    state = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/demographic/src_files/state/ACSDP5Y2024.DP05-Data.csv")
    national = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/demographic/src_files/national/ACSDP5Y2024.DP05-Data.csv")

    df = msa.union(congressional).union(county).union(state).union(national)
    
    return df
    