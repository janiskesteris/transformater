import time
from optparse import OptionParser
from bm_product_catalog_etl.bm_product_catalog_etl import BmProductCatalogEtl
from bm_product_catalog_etl.config import LOGGER

if __name__ == "__main__":
    # big_file_gen('product_catalog.csv', 300)
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
    BmProductCatalogEtl(options.s3_bucket, options.s3_file_path).run()
    end = time.time()
    LOGGER.info("total execution time: {}s".format(round(end - start, 3)))

# TODO add flake8 tests and config
# can connect to HDFS cluster

# nice to have
# TODO setup socket connection between readers and writers

# 8,000,000 - 7.3gb input
# max mem peak 200mb
# exec time 5.52 min

# python main.py --s3bucket=backmarket-data-jobs --s3path=data/product_catalog.csv
