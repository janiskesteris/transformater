import time
from optparse import OptionParser
import os
import timeout_decorator
from retry import retry
from pyarrow import fs

from transformater.input_stream_reader import InputStreamReader
from transformater.output_stream_writer import OutputStreamWriter
from transformater.product_data_cleaner import ProductDataCleaner
from transformater.config import LOGGER

def transform(s3_bucket, s3_file_path, s3_region):
    try:
        file_stream = s3_file_stream(s3_bucket, s3_file_path, s3_region)

        input_stream_reader = InputStreamReader(file_stream)
        input_stream_reader.validate_schema()
        LOGGER.info("schema validated")

        output_stream_writer = OutputStreamWriter(input_stream_reader.schema)
        product_data_cleaner = ProductDataCleaner()

        rows_processed = 0
        for i, batch in input_stream_reader.batches():
            valid_batch, invalid_batch = product_data_cleaner.filter(batch)
            output_stream_writer.write(valid_batch, invalid_batch)
            rows_processed += batch.num_rows
            LOGGER.info(
                "batch {} processed, total processed rows: {}".format(
                    i, rows_processed
                )
            )
    finally:
        file_stream.close()
        if "output_stream_writer" in locals():
            output_stream_writer.close()
        LOGGER.info("cleanup completed")

def s3_file_stream(s3_bucket, s3_file_path, s3_region):
    s3_client = fs.S3FileSystem(region=s3_region, anonymous=True)
    return s3_client.open_input_stream('{}/{}'.format(s3_bucket, s3_file_path))

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option(
        "-b", "--s3_bucket", help="S3 bucket name", default="backmarket-data-jobs"
    )
    parser.add_option(
        "-r", "--s3_region", help="S3 bucket region", default="eu-west-1"
    )
    parser.add_option(
        "-f",
        "--s3_file_path",
        default="data/product_catalog.csv",
        help="S3 path for csv file",
    )
    (options, args) = parser.parse_args()

    start = time.time()
    transform(options.s3_bucket, options.s3_file_path, options.s3_region)
    end = time.time()
    LOGGER.info("total execution time: {}s".format(round(end - start, 3)))

# nice to have
# TODO setup socket connection between readers and writers

# 8,000,000 - 7.3gb input
# max mem peak 200mb
# exec time 5.52 min

# python main.py --s3bucket=backmarket-data-jobs --s3path=data/product_catalog.csv
