import time
from optparse import OptionParser
from transformater.transformater import Transformater
from transformater.config import LOGGER


from pyarrow import fs
from pyarrow import csv
# s3 = fs.S3FileSystem(region='eu-west-1', anonymous=True)
# f = s3.open_input_stream('backmarket-data-jobs/data/product_catalog.csv')

# s3 = fs.S3FileSystem(region='eu-central-1', anonymous=True)
# f = s3.open_input_stream('janis-test-bucket/product_catalog_big.csv')
# start = time.time()
# csv.open_csv(f)
# end = time.time()
# LOGGER.info("total execution time: {}s".format(round(end - start, 3)))
# exit()

# [method_name for method_name in dir(f) if callable(getattr(f, method_name))]
# [method_name for method_name in dir(s3) if callable(getattr(s3, method_name))]
# f


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option(
        "-b", "--s3_bucket", help="S3 bucket name", default="backmarket-data-jobs"
    )
    parser.add_option(
        "-f",
        "--s3_file_path",
        default="data/product_catalog.csv",
        help="S3 path for csv file",
    )
    (options, args) = parser.parse_args()

    start = time.time()
    Transformater(options.s3_bucket, options.s3_file_path).transform()
    end = time.time()
    LOGGER.info("total execution time: {}s".format(round(end - start, 3)))

# nice to have
# TODO setup socket connection between readers and writers

# 8,000,000 - 7.3gb input
# max mem peak 200mb
# exec time 5.52 min

# python main.py --s3bucket=backmarket-data-jobs --s3path=data/product_catalog.csv
