import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME",
                                     "bucket_name",
                                     "input_location",
                                     "output_location",
                                     ])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET_NAME = args["bucket_name"]
INPUT_LOCATION = args["input_location"]
OUTPUT_LOCATION = args["output_location"]

# Data source node - S3 bucket
AmazonS3_source_node = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"s3://{BUCKET_NAME}/{INPUT_LOCATION}"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_source_node",
)

# Data target node - S3 bucket
AmazonS3_target_node = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_source_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f"s3://{BUCKET_NAME}/{OUTPUT_LOCATION}",
        "partitionKeys": ["point_id"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_target_node",
)

job.commit()