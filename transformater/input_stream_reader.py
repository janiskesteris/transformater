import pyarrow.csv
from .config import ROOT_DIR
import os


class InputStreamReader:
    def __init__(self, file_stream):
        self.file_stream = file_stream
        self._stream = None

    def batches(self):
        i = 0
        while True:
            try:
                i += 1
                yield i, self.stream.read_next_batch()
            except StopIteration:
                break

    def validate_schema(self):
        if not self.schema.equals(self.__expected_schema()):
            raise Exception("Invalid scehma for input CSV file")

    def __expected_schema(self):
        schema_file_path = os.path.join(ROOT_DIR, "schema")
        return pyarrow.parquet.read_schema(schema_file_path)

    @property
    def schema(self):
        return self.stream.schema

    @property
    def stream(self):
        if not self._stream:
            # block size for 10mb batches
            read_options = pyarrow.csv.ReadOptions(block_size=10000000)
            self._stream = pyarrow.csv.open_csv(self.file_stream, read_options=read_options)
        return self._stream
