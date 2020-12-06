import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.csv
from .config import ROOT_DIR
import os

class InputStreamReader:
    def __init__(self, local_file_path):
        self.local_file_path = local_file_path
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

    def cleanup(self):
        if(os.path.exists(self.local_file_path)):
            os.remove(self.local_file_path)

    def __expected_schema(self):
        schema_file_path = os.path.join(ROOT_DIR, 'schema')
        return pyarrow.parquet.read_schema(schema_file_path)

    @property
    def schema(self):
        return self.stream.schema

    @property
    def stream(self):
        if not self._stream:
            read_options = pyarrow.csv.ReadOptions(block_size=10000000) # 10 mb batches
            self._stream = pyarrow.csv.open_csv(self.local_file_path, read_options=read_options)
        return self._stream

