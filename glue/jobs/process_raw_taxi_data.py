#!/usr/bin/env python
# coding: utf-8

# In[24]:


import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
is_prod = False

try:
    # Prod
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'file_key', 'source_bucket', 'target_bucket'])
    file_key = args['file_key']
    source_bucket = args['source_bucket']
    target_bucket = args['target_bucket']
    is_prod = True
    job.init(args['JOB_NAME'] + file_key, args)
except:
    # Dev
    file_key = 'year=2023/month=01/data.parquet'
    source_bucket = 'raw-taxi-data'
    target_bucket = 'transformed-taxi-data'


# In[25]:


df = spark.read.parquet(f's3://{source_bucket}/{file_key}')


# **Handle sceneario where datetimes are defined as `Long` containing nanoseconds since epoch**

# In[26]:


from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import LongType

if isinstance(df.schema['tpep_pickup_datetime'].dataType, LongType):
    df = df \
    .withColumn('tpep_pickup_datetime', from_unixtime(col('tpep_pickup_datetime') / 1_000_000_000).cast('timestamp')) \
    .withColumn('tpep_dropoff_datetime', from_unixtime(col('tpep_dropoff_datetime') / 1_000_000_000).cast('timestamp'))


# #### Cleaning

# In[27]:


from pyspark.sql.functions import when, col, udf, monotonically_increasing_id


# **Remove rows with null in important columns**

# In[28]:


df = df.na.drop(subset=['total_amount', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance'])


# **Filter out-of-range values**

# In[29]:


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

# In[30]:


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


# **Outlier handling and tranformation**

# In[31]:


from pyspark.sql.types import FloatType, IntegerType

def get_kilometers_from_miles(miles):
    return float("{:.3f}".format(miles * 1.60934))

udf_get_kilometers_from_miles = udf(get_kilometers_from_miles, FloatType())

df = df.withColumn('vendor_id', when(col('vendor_id').isin([1, 2]), col('vendor_id')).otherwise(None)) \
    .withColumn('store_and_forward', when(col('store_and_forward') == 'Y', True).otherwise(False)) \
    .withColumn('rate_code_id', when(col('rate_code_id').isin(list(range(1,7))), col('rate_code_id')).otherwise(None))  \
    .withColumn('payment_type_id', when(col('payment_type_id').isin(list(range(1,7))), col('payment_type_id')).otherwise(None)) \
    .withColumn('trip_distance', udf_get_kilometers_from_miles(col('trip_distance')))


# **Generate trip_id**

# In[32]:


import uuid


# In[33]:


generate_trip_id = udf(lambda *cols: str(uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(str(col) for col in cols))))

df = df.withColumn('trip_id', monotonically_increasing_id())

columns_for_id_generation = [df[column] for column in df.columns]

df = df.withColumn('trip_id', generate_trip_id(*columns_for_id_generation))


# **Get dates and times id**

# In[34]:


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


# **Cast types**

# In[35]:


from awsglue.dynamicframe import DynamicFrame

if not is_prod:
    df = df.limit(10000)

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")


# In[36]:


dyf.printSchema()


# In[37]:


dyf = dyf.apply_mapping([
    ('trip_id', 'string', 'trip_id', 'string'),
    ('vendor_id', 'long', 'vendor_id', 'int'),
    ('pickup_date_id', 'int', 'pickup_date_id', 'int'),
    ('pickup_time_id', 'int', 'pickup_time_id', 'int'),
    ('dropoff_date_id', 'int', 'dropoff_date_id', 'int'),
    ('dropoff_time_id', 'int', 'dropoff_time_id', 'int'),
    ('passenger_count', 'double', 'passenger_count', 'int'),
    ('trip_distance', 'float', 'trip_distance', 'float'),
    ('pickup_location_id', 'long', 'pickup_location_id', 'int'),
    ('dropoff_location_id', 'long', 'dropoff_location_id', 'int'),
    ('rate_code_id', 'double', 'rate_code_id', 'int'),
    ('store_and_forward', 'boolean', 'store_and_forward', 'boolean'),
    ('payment_type_id', 'long', 'payment_type_id', 'int'),
    ('fare_amount', 'double', 'fare_amount', 'int'),
    ('extra_amount', 'double', 'extra_amount', 'float'),
    ('mta_tax', 'double', 'mta_tax', 'float'),
    ('tip_amount', 'double', 'tip_amount', 'float'),
    ('tolls_amount', 'double', 'tolls_amount', 'float'),
    ('improvement_surcharge', 'double', 'improvement_surcharge', 'float'),
    ('congestion_surcharge', 'double', 'congestion_surcharge', 'float'),
    ('airport_fee_amount', 'double', 'airport_fee_amount', 'float'),
    ('total_amount', 'double', 'total_amount', 'float'),
])


# In[38]:


folder_path = '/'.join(file_key.split('/')[:-1])

glueContext.write_dynamic_frame.from_options(
    frame=dyf.repartition(2),
    connection_type='s3',
    format='csv',
    connection_options={'path': f's3://{target_bucket}/{folder_path}'},
    transformation_ctx='transformed_taxi_data_bucket',
)


# In[42]:


job.commit() if is_prod else print("""
Take this $5 pesitos
(づ｡◕‿‿◕｡)づ [̲̅$̲̅(̲̅5̲̅)̲̅$̲̅]  (◕‿◕✿)

What am I gonna do with only $5 pesitos?
┻━┻ ︵ヽ(`Д´)ﾉ︵ ┻━┻

Invest in Bolivares, it's going to the moon [̲̅$̲̅(̲̅5̲̅^1e10)̲̅$̲̅]
(づ｡◕‿‿◕｡)づ

But...
(ಥ﹏ಥ)
""")

