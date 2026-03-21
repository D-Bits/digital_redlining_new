import pyspark.sql.functions as F
import pyspark.pipelines as dp
import requests


@dp.materialized_view(name="census_bronze.raw_demographic_national")
def raw_national_demographic():

    # Extract JSON
    res = requests.get("https://api.census.gov/data/2020/dec/pl?get=GEO_ID,NAME,P1_001N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N,P1_009N,P2_001N,P2_002N,H1_001N,H1_002N&ucgid=0100000US").json()
    columns = res[0]
    data = res[1:]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    return df


@dp.materialized_view(name="bronze_census.raw_demographic_state")
def raw_state_demographic():

    # Extract JSON
    res = requests.get("https://api.census.gov/data/2020/dec/pl?get=GEO_ID,NAME,P1_001N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N,P1_009N,P2_001N,P2_002N,H1_001N,H1_002N&for=state").json()
    columns = res[0]
    data = res[1:]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    return df


@dp.materialized_view(name="bronze_census.raw_demographic_county")
def raw_county_demographic():

    # Extract JSON
    res = requests.get("https://api.census.gov/data/2020/dec/pl?get=GEO_ID,NAME,P1_001N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N,P1_009N,P2_001N,P2_002N,H1_001N,H1_002N&for=county").json()
    columns = res[0]
    data = res[1:]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    return df


@dp.materialized_view(name="bronze_census.raw_demographic_msa")
def raw_msa_demographic():

    # Extract JSON
    res = requests.get("https://api.census.gov/data/2020/dec/pl?get=GEO_ID,NAME,P1_001N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N,P1_009N,P2_001N,P2_002N,H1_001N,H1_002N&ucgid=pseudo(0100000US$3100000)").json()
    columns = res[0]
    data = res[1:]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    return df
