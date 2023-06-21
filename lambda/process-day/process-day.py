import netCDF4 as nc
import numpy as np
import pandas as pd
from datetime import datetime
import time
import os
import random
import logging
import boto3

# Bucket containing input data
INPUT_BUCKET_NAME = os.environ["BUCKET_NAME"]  # example: "my-bucket-name"
LOCATION = os.environ["INPUT_LOCATION"]  # example: "MSG/MDSSFTD/NETCDF/"

# Local output files
TMP_FILE_NAME = "/tmp/tmp.nc"
LOCAL_OUTPUT_FILE = "/tmp/dataframe.parquet"

# Bucket for output data
OUTPUT_BUCKET = os.environ["BUCKET_NAME"]
OUTPUT_PREFIX = os.environ["OUTPUT_LOCATION"]  # example: "output/intermediate/"

# Create 100 random coordinates
random.seed(10)
coords = [(random.randint(1000, 2500), random.randint(1000, 2500)) for _ in range(100)]

client = boto3.resource("s3")
bucket = client.Bucket(INPUT_BUCKET_NAME)


def date_to_partition_name(date):
    """
    Transform a date like "20180302" to partition like "2018/03/02/"
    """
    d = datetime.strptime(date, "%Y%m%d")
    return d.strftime("%Y/%m/%d/")


def lambda_handler(event, context):
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Get date from input
    date = str(event)
    logger.info("Processing date: %s", date)

    # Initialize output dataframe
    COLUMNS_NAME = ["time", "point_id", "DSSF_TOT", "FRACTION_DIFFUSE"]
    df = pd.DataFrame(columns=COLUMNS_NAME)

    prefix = LOCATION + date_to_partition_name(date)
    logger.info("Loading files from prefix: %s", prefix)

    # List input files (weather files)
    objects = bucket.objects.filter(Prefix=prefix)
    keys = [obj.key for obj in objects]

    # For each file
    for key in keys:
        # Download input file from S3
        bucket.download_file(key, TMP_FILE_NAME)

        logger.info("Processing: %s", key)

        try:
            # Load the dataset with netcdf library
            dataset = nc.Dataset(TMP_FILE_NAME)

            # Get values from the dataset for our list of geographical coordinates
            lats, lons = zip(*coords)
            data_1 = dataset["DSSF_TOT"][0][lats, lons]
            data_2 = dataset["FRACTION_DIFFUSE"][0][lats, lons]

            # Prepare data to add it into the output dataframe
            nb_points = len(lats)
            data_time = dataset.__dict__["time_coverage_start"]
            time_list = [data_time for _ in range(nb_points)]
            point_id_list = [i for i in range(nb_points)]
            tuple_list = list(zip(time_list, point_id_list, data_1, data_2))

            # Add data to the output dataframe
            new_data = pd.DataFrame(tuple_list, columns=COLUMNS_NAME)
            df = pd.concat([df, new_data])
        except OSError:
            logger.error("Error processing file: %s", key)

    # Replace masked by NaN (otherwise we cannot save to parquet)
    df = df.applymap(lambda x: np.NaN if type(x) == np.ma.core.MaskedConstant else x)

    # Save to parquet
    logger.info("Writing result to tmp parquet file: %s", LOCAL_OUTPUT_FILE)
    df.to_parquet(LOCAL_OUTPUT_FILE)

    # Copy result to S3
    s3_output_name = OUTPUT_PREFIX + date + ".parquet"
    s3_client = boto3.client("s3")
    s3_client.upload_file(LOCAL_OUTPUT_FILE, OUTPUT_BUCKET, s3_output_name)
