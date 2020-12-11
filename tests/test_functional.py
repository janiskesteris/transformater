import os
import glob
import pytest
import pyarrow.parquet as pq
import numpy as np
from unittest import mock
from unittest.mock import patch
from transformater.config import ROOT_DIR
from transformater.input_stream_reader import InputStreamReader
from transformater.input_stream_reader import MAX_RETRIES, SLEEP_BETWEEN_TRIES

test_data_path = os.path.join(ROOT_DIR, "..", "tests", "data")
fixture_path = os.path.join(ROOT_DIR, "..", "tests", "fixtures")
s3_bucker, s3_file_path, s3_region = (
    "backmarket-data-jobs",
    "data/product_catalog.csv",
    "eu-west-1",
)


def test_integration():
    with mock.patch("transformater.config.DATA_DIR", test_data_path):
        from transform import transform

        transform(s3_bucker, s3_file_path, s3_region)
        valid_table = pq.read_table(
            os.path.join(test_data_path, "valid_products.parquet")
        )
        invalid_table = pq.read_table(
            os.path.join(test_data_path, "invalid_products.parquet")
        )

        # check valid and invalid product count adds up to initial
        assert valid_table.num_rows + invalid_table.num_rows == 1000

        # check if valid product count is correct
        assert (
            valid_table.to_pandas()["image"].replace("", np.nan).dropna().count()
            == valid_table.num_rows
        )

        # check that products in invalid set has no images
        assert (
            invalid_table.to_pandas()["image"].replace("", np.nan).dropna().count() == 0
        )
    cleanup()

@patch("transformater.input_stream_reader.MAX_RETRIES", 2)
@patch("transformater.input_stream_reader.SLEEP_BETWEEN_TRIES", 0)
@mock.patch.object(InputStreamReader, '_InputStreamReader__next_batch')
def test_connection_drop_recovery(next_patch):
    # raise error 2 times and then recover
    next_patch.side_effect = mock.Mock(side_effect=[
        OSError("Couldn't resolve host name"),
        OSError("Couldn't resolve host name"),
        1
    ])
    reader = InputStreamReader('')
    [a for a in reader.batches()]

@patch("transformater.input_stream_reader.MAX_RETRIES", 2)
@patch("transformater.input_stream_reader.SLEEP_BETWEEN_TRIES", 0)
@mock.patch.object(InputStreamReader, '_InputStreamReader__next_batch')
def test_connection_drop_fail(next_patch):
    # raise error 3 times and then recover
    next_patch.side_effect = mock.Mock(side_effect=[
        OSError("Couldn't resolve host name"),
        OSError("Couldn't resolve host name"),
        OSError("Couldn't resolve host name"),
        1
    ])
    with pytest.raises(OSError, match=r"Couldn't resolve host name"):
        reader = InputStreamReader('')
        [a for a in reader.batches()]

def test_schema_validation():
    file_path = os.path.join(fixture_path, "wrong_schema.csv")
    with pytest.raises(Exception, match=r"Invalid scehma for input CSV file"):
        InputStreamReader(file_path).validate_schema()


def cleanup():
    for file in glob.glob(os.path.join(test_data_path, "*")):
        os.remove(file)