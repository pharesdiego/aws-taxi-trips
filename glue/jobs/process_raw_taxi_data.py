#!/usr/bin/env python
# coding: utf-8

# In[558]:


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# In[559]:

dyf = glueContext.create_dynamic_frame.from_catalog(
    database='taxitripsdb',
    table_name='raw_taxi_data',
    transformation_ctx='dyf',
)


df = dyf.toDF()

# ## Cleaning

# In[562]:


from pyspark.sql.functions import when, col, udf, monotonically_increasing_id


# **Remove rows with null in important columns**

# In[563]:


df = df.na.drop(subset=['total_amount', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance'])


# **Filter out-of-range values**

# In[564]:


# [column, min_value, max_value]
cols_ranges = [
    ['passenger_count', '0', '6'],
    ['fare_amount', '0.1', None],
    ['extra', '0', None],
    ['mta_tax', '0', None],
    ['tip_amount', '0', None],
    ['tolls_amount', '0', None],
    ['improvement_surcharge', '0', None],
    ['total_amount', '3', None],
    ['congestion_surcharge', '0', None],
    ['trip_distance', '0', '100'],
    ['tpep_pickup_datetime', '"2023-01-01"', None],
    ['tpep_dropoff_datetime', None, '"2023-12-31"']
]

filter_str = 'airport_fee = 1.25 OR airport_fee = 0 AND tpep_pickup_datetime < tpep_dropoff_datetime AND '

for [col_name, min_value, max_value] in cols_ranges:
    filter_str += f'{col_name} >= {min_value} AND ' if min_value != None else ''
    filter_str += f'{col_name} <= {max_value} AND ' if max_value != None else ''
    
df = df.filter(filter_str[:-5])


# **Standardize columns name**

# In[565]:


columns_to_be_renamed = [
    ['VendorID', 'vendor_id'],
    ['tpep_pickup_datetime', 'pickup_datetime'],
    ['tpep_dropoff_datetime', 'dropoff_datetime'],
    ['RatecodeID', 'rate_code_id'],
    ['store_and_fwd_flag', 'store_and_forward'],
    ['PULocationID', 'pickup_location_id'],
    ['DOLocationID', 'dropoff_location_id'],
    ['extra', 'extra_amount'],
    ['Airport_fee', 'airport_fee_amount'],
    ['payment_type', 'payment_type_id']
]

for [old_name, new_name] in columns_to_be_renamed:
    df = df.withColumnRenamed(old_name, new_name)


# **Cast types**

# In[566]:


from pyspark.sql.types import FloatType, IntegerType


# In[567]:


for [col_name, col_type] in df.dtypes:
    if col_type == 'double':
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))

    if col_type == 'bigint':
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

# **Outlier handling and tranformation**

# In[569]:


def get_kilometers_from_miles(miles):
    return float("{:.3f}".format(miles * 1.60934))

udf_get_kilometers_from_miles = udf(get_kilometers_from_miles, FloatType())

df = df.withColumn('vendor_id', when(col('vendor_id').isin([1, 2]), col('vendor_id')).otherwise(None)) \
    .withColumn('store_and_forward', when(col('store_and_forward') == 'Y', True).otherwise(False)) \
    .withColumn('rate_code_id', when(col('rate_code_id').isin(list(range(1,7))), col('rate_code_id')).otherwise(None))  \
    .withColumn('payment_type_id', when(col('payment_type_id').isin(list(range(1,7))), col('payment_type_id')).otherwise(None)) \
    .withColumn('trip_distance', udf_get_kilometers_from_miles(col('trip_distance')))


# **Generate trip_id**

# In[570]:


import uuid


# In[571]:


generate_trip_id = udf(lambda *cols: str(uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(str(col) for col in cols))))

df = df.withColumn('trip_id', monotonically_increasing_id())

columns_for_id_generation = [df[column] for column in df.columns]

df = df.withColumn('trip_id', generate_trip_id(*columns_for_id_generation))


# **Get dates and times id**

# In[572]:


def get_date_id_from_datetime(dt):
    return dt.timetuple().tm_yday - 1
    
def get_time_id_from_datetime(dt):
    tt = dt.timetuple()
    return tt.tm_hour * 3600 + tt.tm_min * 60 + tt.tm_sec

udf_get_date_id_from_datetime = udf(get_date_id_from_datetime)
udf_get_time_id_from_datetime = udf(get_time_id_from_datetime)

df = df \
    .withColumn('pickup_date_id', udf_get_date_id_from_datetime(col('pickup_datetime')).cast(IntegerType())) \
    .withColumn('dropoff_date_id', udf_get_date_id_from_datetime(col('dropoff_datetime')).cast(IntegerType())) \
    .withColumn('pickup_time_id', udf_get_time_id_from_datetime(col('pickup_datetime')).cast(IntegerType())) \
    .withColumn('dropoff_time_id', udf_get_time_id_from_datetime(col('dropoff_datetime')).cast(IntegerType())) 

df = df.select(*filter(lambda c: '_datetime' not in c, df.columns))


# **Rearrange columns**

# In[573]:


df = df.select(*[
    'trip_id',
    'vendor_id',
    'pickup_date_id',
    'pickup_time_id',
    'dropoff_date_id',
    'dropoff_time_id',
    'passenger_count',
    'trip_distance',
    'pickup_location_id',
    'dropoff_location_id',
    'rate_code_id',
    'store_and_forward',
    'payment_type_id',
    'fare_amount',
    'extra_amount',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'congestion_surcharge',
    'airport_fee_amount',
    'total_amount',
    'partition_0',
    'partition_1'
])

# In[576]:

from awsglue.dynamicframe import DynamicFrame

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

transformed_taxi_data_bucket = glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type='s3',
    format='csv',
    connection_options={'path': 's3://transformed-taxi-data', 'partitionKeys': ['partition_0', 'partition_1']},
    transformation_ctx='transformed_taxi_data_bucket',
)

job.commit()
