from pyarrow.parquet import ParquetWriter
from pyarrow import Table
import os
from .config import DATA_DIR

class OutputStreamWriter:
    def __init__(self, schema):
        self.valid_writer = ParquetWriter(self.parquet_file_path('valid_products'), schema)
        self.invalid_writer = ParquetWriter(self.parquet_file_path('invalid_products'), schema)

    def write(self, valid_batch, invalid_batch):
        self.valid_writer.write_table(Table.from_batches([valid_batch]))
        self.invalid_writer.write_table(Table.from_batches([invalid_batch]))

    def parquet_file_path(self, file_basename):
        return os.path.join(DATA_DIR, '{}.parquet'.format(file_basename))

    def close(self):
        self.valid_writer.close()
        self.invalid_writer.close()
