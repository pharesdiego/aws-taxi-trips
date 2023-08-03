import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1691077411587 = glueContext.create_dynamic_frame.from_catalog(
    database="taxitripsdb",
    table_name="transformed_taxi_data",
    transformation_ctx="AWSGlueDataCatalog_node1691077411587",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=AWSGlueDataCatalog_node1691077411587,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-176256382487-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.fact_trips",
        "connectionName": "redshift_conn"
    },
    transformation_ctx="AmazonRedshift_node3",
)

job.commit()
