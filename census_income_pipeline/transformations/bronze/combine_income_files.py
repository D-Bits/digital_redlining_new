from pyspark import pipelines as dp


@dp.materialized_view(name="census_income.raw_combined")
def raw_combined():

    # Load source files into memory
    msa = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_income/src_files/ACSST5Y2024.S1903_2026-03-10_msa/ACSST5Y2024.S1903-Data.csv")
    congressional = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_income/src_files/ACSST5Y2024.S1903_2026-03-10_congressional/ACSST5Y2024.S1903-Data.csv")
    county = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_income/src_files/ACSST5Y2024.S1903_2026-03-10_state/ACSST5Y2024.S1903-Data.csv")
    state = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_income/src_files/ACSST5Y2024.S1903_2026-03-10_state/ACSST5Y2024.S1903-Data.csv")
    national = spark.read.format("csv").option("header", "true").load("/Volumes/digital_redlining/census_income/src_files/ACSST5Y2024.S1903_2026-03-05_national/ACSST5Y2024.S1903-Data.csv")

    df = msa.union(congressional).union(county).union(state).union(national)
    
    return df
    