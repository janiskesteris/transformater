from bm_product_catalog_etl.s3_file_downloader import S3FileDownloader
from bm_product_catalog_etl.input_stream_reader import InputStreamReader
from bm_product_catalog_etl.output_stream_writer import OutputStreamWriter
from bm_product_catalog_etl.product_data_cleaner import ProductDataCleaner
from bm_product_catalog_etl.config import LOGGER

class BmProductCatalogEtl:
    def __init__(self, s3_bucket, s3_file_path):
        self.s3_bucket = s3_bucket
        self.s3_file_path = s3_file_path

    def run(self):
        try:
            local_file_path = self.__download_file()

            input_stream_reader = InputStreamReader(local_file_path)
            input_stream_reader.validate_schema()
            LOGGER.info("schema validated")

            output_stream_writer = OutputStreamWriter(input_stream_reader.schema)
            product_data_cleaner = ProductDataCleaner()

            rows_processed = 0
            for i, batch in input_stream_reader.batches():
                valid_batch, invalid_batch = product_data_cleaner.filter(batch)
                output_stream_writer.write(valid_batch, invalid_batch)
                rows_processed += batch.num_rows
                LOGGER.info("batch {} processed, total processed rows: {}".format(i, rows_processed))
        finally:
            if 'input_stream_reader' in locals():
                input_stream_reader.cleanup()
            if 'output_stream_writer' in locals():
                output_stream_writer.close()
            LOGGER.info("cleanup completed")


    def __download_file(self):
        file_downloader = S3FileDownloader(self.s3_bucket, self.s3_file_path)
        file_downloader.download()
        LOGGER.info("CSV file downloaded at {}".format(file_downloader.local_file_path()))
        return file_downloader.local_file_path()