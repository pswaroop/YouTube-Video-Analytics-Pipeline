import re
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def iso_duration_to_seconds(duration):
    if not duration:
        return 0
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.fullmatch(duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    
    return hours*3600+minutes*60+seconds

duration_to_sec_udf = udf(iso_duration_to_seconds, IntegerType())