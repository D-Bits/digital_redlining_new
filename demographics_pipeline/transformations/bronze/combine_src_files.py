from pyspark import pipelines as dp


@dp.materialized_view(name="raw_combined")
def raw_combined():

    # Load and sanitize column names
    def sanitize(df):
        forbidden_chars = [" ", ",", ";", "{", "}", "(", ")", "=", "\n", "\t"]
        for col in df.columns:
            new_col = col
            for char in forbidden_chars:
                new_col = new_col.replace(char, "_")
            df = df.withColumnRenamed(col, new_col)
        return df

    msa = sanitize(spark.read.format("csv").option("header", "true").option("columnNameMapping", "true").load("/Volumes/digital_redlining/demographic/src_files/msa/ACSDP5Y2024.DP05-Data.csv"))
    congressional = sanitize(spark.read.format("csv").option("header", "true").option("columnNameMapping", "true").load("/Volumes/digital_redlining/demographic/src_files/congressional/ACSDP5Y2024.DP05-Data.csv"))
    county = sanitize(spark.read.format("csv").option("header", "true").option("columnNameMapping", "true").load("/Volumes/digital_redlining/demographic/src_files/county/ACSDP5Y2024.DP05-Data.csv"))
    state = sanitize(spark.read.format("csv").option("header", "true").option("columnNameMapping", "true").load("/Volumes/digital_redlining/demographic/src_files/state/ACSDP5Y2024.DP05-Data.csv"))
    national = sanitize(spark.read.format("csv").option("header", "true").option("columnNameMapping", "true").load("/Volumes/digital_redlining/demographic/src_files/national/ACSDP5Y2024.DP05-Data.csv"))

    df = msa.union(congressional).union(county).union(state).union(national)
    
    return df
    