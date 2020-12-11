import pyarrow.csv
from .config import ROOT_DIR, LOGGER, BATCH_BLOCK_SIZE
import os
from time import sleep
MAX_RETRIES = 5
SLEEP_BETWEEN_TRIES = 10


class InputStreamReader:
    def __init__(self, file_stream):
        self.file_stream = file_stream
        self._stream = None

    def batches(self):
        i = tries = 0
        while True:
            try:
                batch = self.__next_batch()
                i += 1
                yield i, batch
            except StopIteration:
                break
            except OSError as err:
                if "Couldn't resolve host name" in str(err) and tries < MAX_RETRIES:
                    tries += 1
                    LOGGER.error("connection to s3 failed, retrying...")
                    sleep(SLEEP_BETWEEN_TRIES)
                    continue
                raise

    def validate_schema(self):
        if not self.schema.equals(self.__expected_schema()):
            raise Exception("Invalid scehma for input CSV file")

    def __next_batch(self):
        return self.stream.read_next_batch()

    def __expected_schema(self):
        schema_file_path = os.path.join(ROOT_DIR, "schema")
        return pyarrow.parquet.read_schema(schema_file_path)

    @property
    def schema(self):
        return self.stream.schema

    @property
    def stream(self):
        if not self._stream:
            read_options = pyarrow.csv.ReadOptions(block_size=BATCH_BLOCK_SIZE)
            self._stream = pyarrow.csv.open_csv(
                self.file_stream, read_options=read_options
            )
        return self._stream
