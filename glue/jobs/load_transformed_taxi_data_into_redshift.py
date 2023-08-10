# Literally made by moving blocks around (▀̿Ĺ̯▀̿ ̿)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue import DynamicFrame


def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="taxitripsdb",
    table_name="transformed_taxi_data",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Change Schema
ChangeSchema_node1691706181322 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("trip_id", "string", "trip_id", "string"),
        ("vendor_id", "long", "vendor_id", "int"),
        ("pickup_date_id", "long", "pickup_date_id", "int"),
        ("pickup_time_id", "long", "pickup_time_id", "int"),
        ("dropoff_date_id", "long", "dropoff_date_id", "int"),
        ("dropoff_time_id", "long", "dropoff_time_id", "int"),
        ("passenger_count", "long", "passenger_count", "int"),
        ("trip_distance", "double", "trip_distance", "double"),
        ("pickup_location_id", "long", "pickup_location_id", "int"),
        ("dropoff_location_id", "long", "dropoff_location_id", "int"),
        ("rate_code_id", "long", "rate_code_id", "int"),
        ("store_and_forward", "boolean", "store_and_forward", "boolean"),
        ("payment_type_id", "long", "payment_type_id", "int"),
        ("fare_amount", "long", "fare_amount", "double"),
        ("extra_amount", "double", "extra_amount", "double"),
        ("mta_tax", "double", "mta_tax", "double"),
        ("tip_amount", "double", "tip_amount", "double"),
        ("tolls_amount", "double", "tolls_amount", "double"),
        ("improvement_surcharge", "double", "improvement_surcharge", "double"),
        ("congestion_surcharge", "double", "congestion_surcharge", "double"),
        ("airport_fee_amount", "double", "airport_fee_amount", "double"),
        ("total_amount", "double", "total_amount", "double"),
    ],
    transformation_ctx="ChangeSchema_node1691706181322",
)

# Script generated for node Drop Null Fields
DropNullFields_node1691704933528 = drop_nulls(
    glueContext,
    frame=ChangeSchema_node1691706181322,
    nullStringSet={},
    nullIntegerSet={},
    transformation_ctx="DropNullFields_node1691704933528",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropNullFields_node1691704933528,
    database="taxitripsdb",
    table_name="taxi_trips_public_fact_trips",
    redshift_tmp_dir="s3://aws-glue-assets-176256382487-us-east-1/temporary/",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
