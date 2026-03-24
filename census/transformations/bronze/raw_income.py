from pyspark import pipelines as dp
import pyspark.sql.functions as F
import pyspark.pipelines as dp
import requests


@dp.materialized_view(name="bronze_census.raw_income_state")
def raw_income_state():

    # Census ACS variables to be written to storage for each geo level 
    variables = [
    'GEO_ID',
    'NAME',  
    'S1903_C01_001E', 
    'S1903_C01_002E', 
    'S1903_C01_003E', 
    'S1903_C01_009E', 
    'S1903_C01_007E', 
    'S1903_C01_006E', 
    'S1903_C01_005E', 
    'S1903_C01_004E',
    ]

    res = requests.get("https://api.census.gov/data/2024/acs/acs5/subject?get=GEO_ID,NAME,S1903_C01_001E,S1903_C01_002E,S1903_C01_009E,S1903_C01_007E,S1903_C01_006E,S1903_C01_005E,S1903_C01_004E&ucgid=pseudo(0100000US$0400000)").json()
    columns = res[0]
    data = res[1:]

    df = spark.createDataFrame(data, columns)

    return df


@dp.materialized_view(name="bronze_census.raw_income_county")
def raw_income_county():

    # Census ACS variables to be written to storage for each geo level 
    variables = [
    'GEO_ID',
    'NAME',  
    'S1903_C01_001E', 
    'S1903_C01_002E', 
    'S1903_C01_003E', 
    'S1903_C01_009E', 
    'S1903_C01_007E', 
    'S1903_C01_006E', 
    'S1903_C01_005E', 
    'S1903_C01_004E',
    ]

    res = requests.get("https://api.census.gov/data/2024/acs/acs5/subject?get=GEO_ID,NAME,S1903_C01_001E,S1903_C01_002E,S1903_C01_009E,S1903_C01_007E,S1903_C01_006E,S1903_C01_005E,S1903_C01_004E&ucgid=pseudo(0100000US$0500000)").json()
    columns = res[0]
    data = res[1:]

    df = spark.createDataFrame(data, columns)

    return df


@dp.materialized_view(name="bronze_census.raw_income_msa")
def raw_income_msa():

    # Census ACS variables to be written to storage for each geo level 
    variables = [
    'GEO_ID',
    'NAME',  
    'S1903_C01_001E', 
    'S1903_C01_002E', 
    'S1903_C01_003E', 
    'S1903_C01_009E', 
    'S1903_C01_007E', 
    'S1903_C01_006E', 
    'S1903_C01_005E', 
    'S1903_C01_004E',
    ]

    res = requests.get("https://api.census.gov/data/2024/acs/acs5/subject?get=GEO_ID,NAME,S1903_C01_001E,S1903_C01_002E,S1903_C01_009E,S1903_C01_007E,S1903_C01_006E,S1903_C01_005E,S1903_C01_004E&ucgid=pseudo(0100000US$3100000)").json()
    columns = res[0]
    data = res[1:]

    df = spark.createDataFrame(data, columns)

    return df
