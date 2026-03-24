from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re


# Define necessary data validations for bronze pipelines
bronze_validations = {
	'no_null_geoids': 'GEO_ID IS NOT NULL',
}

# Define necessary data validations for silver pipelines
silver_validations = {
	'no_null_geoids': 'geo_id IS NOT NULL',
}


@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None
