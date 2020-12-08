import os
import glob
import pytest
import pyarrow.parquet as pq
import numpy as np
from unittest import mock
from transformater.config import ROOT_DIR
from transformater.input_stream_reader import InputStreamReader

test_data_path = os.path.join(ROOT_DIR, "..", "tests", "data")
fixture_path = os.path.join(ROOT_DIR, "..", "tests", "fixtures")


def test_integration():
    with mock.patch("transformater.config.DATA_DIR", test_data_path):
        # mock cleanup for test comparison
        s3_bucker, s3_file_path, s3_region = (
            "backmarket-data-jobs",
            "data/product_catalog.csv",
            "eu-west-1",
        )

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


def test_schema_validation():
    file_path = os.path.join(fixture_path, "wrong_schema.csv")
    with pytest.raises(Exception, match=r"Invalid scehma for input CSV file"):
        InputStreamReader(file_path).validate_schema()


def cleanup():
    for file in glob.glob(os.path.join(test_data_path, "*")):
        os.remove(file)
