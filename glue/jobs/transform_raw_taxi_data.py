#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


dyf = glueContext.create_dynamic_frame.from_catalog(
    database='taxitripsdb',
    table_name='raw_taxi_data',
    transformation_ctx='dyf',
)

df = dyf.toDF()


# ## Cleaning

# In[3]:


from pyspark.sql.functions import when, col


# **Remove rows with null in important columns**

# In[4]:


df = df.na.drop(subset=['total_amount', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID'])


# **Standardize columns name**

# In[5]:


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


# **Make sure values are within range**

# In[6]:


df = df.withColumn('vendor_id', when(col('vendor_id').isin([1, 2]), col('vendor_id')).otherwise(3))


# In[7]:


df = df.withColumn('store_and_forward', when(col('store_and_forward') == 'Y', True).otherwise(False))


# ## Aggregation

# In[8]:


def get_kilometers_from_miles(miles):
    try:
        return miles * 1.60934
    except:
        return 0

def get_payment_type(payment_type_id):
    _map = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        6: 'Voided trip'
    }

    return _map[payment_type_id] if payment_type_id in _map else 'Unknown'

def get_rate_code(rate_code_id):
    _map = {
        1: 'Standard',
        2: 'Airport (JFK)',
        3: 'Newark',
        4: 'Nassau or Nassau or Westchester',
        5: 'Negotiated',
        6: 'Group ride'
    }

    return _map[rate_code_id] if rate_code_id in _map else 'Unknown'

def get_vendor(vendor_id):
    _map = {
        1: 'Creative Mobile Technologies',
        2: 'VeriFone'
    }

    return _map[vendor_id] if vendor_id in _map else 'Unknown'

def aggregator(df):
    row = df.asDict()
    row['payment_type'] = get_payment_type(row['payment_type_id'])
    row['rate_code'] = get_rate_code(row['rate_code_id'])
    row['vendor'] = get_vendor(row['vendor_id'])
    row['trip_distance'] = get_kilometers_from_miles(row['trip_distance'])

    return row
    

rdd = df.rdd.map(aggregator)
df = rdd.toDF()


# **Add location data**

# In[9]:


df.createOrReplaceTempView('taxi_trips')
df_taxi_zones = spark.read.format('csv').option('header', 'true').load('s3://raw-taxi-data/taxi_zone_lookup.csv')
df_taxi_zones.createOrReplaceTempView('taxi_zones')


# In[10]:


no_ids_columns = filter(lambda column: '_id' not in column, df.columns)

df = spark.sql(f"""
SELECT {','.join(no_ids_columns)}, partition_0, partition_1,
taxi_zones_pickup.Zone as pickup_location_zone, 
taxi_zones_pickup.Borough as pickup_location_borough, 
taxi_zones_dropoff.Zone as dropoff_location_zone, 
taxi_zones_dropoff.Borough as dropoff_location_borough
FROM taxi_trips
JOIN taxi_zones AS taxi_zones_pickup on taxi_zones_pickup.LocationID = taxi_trips.pickup_location_id
JOIN taxi_zones AS taxi_zones_dropoff on taxi_zones_dropoff.LocationID = taxi_trips.dropoff_location_id
""")

# #### Load into transformed_taxi_trips S3

# In[12]:


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