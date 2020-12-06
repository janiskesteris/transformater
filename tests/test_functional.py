import os
import glob
import pytest
import pyarrow.parquet as pq
import pyarrow.csv as csv
import numpy as np
from unittest import mock
from bm_product_catalog_etl.config import ROOT_DIR
from bm_product_catalog_etl.input_stream_reader import InputStreamReader

test_data_path = os.path.join(ROOT_DIR, "..", "tests", "data")
fixture_path = os.path.join(ROOT_DIR, "..", "tests", "fixtures")


def test_integration():
    with mock.patch("bm_product_catalog_etl.config.DATA_DIR", test_data_path):
        # mock cleanup for test comparison
        with mock.patch.object(InputStreamReader, "cleanup"):
            from bm_product_catalog_etl.bm_product_catalog_etl import (
                BmProductCatalogEtl,
            )

            etl = BmProductCatalogEtl(
                "backmarket-data-jobs", "data/product_catalog.csv"
            )
            etl.run()

            raw_csv = csv.read_csv(os.path.join(test_data_path, "product_catalog.csv"))
            valid_table = pq.read_table(
                os.path.join(test_data_path, "valid_products.parquet")
            )
            invalid_table = pq.read_table(
                os.path.join(test_data_path, "invalid_products.parquet")
            )

            # check valid and invalid product count adds up to initial
            assert valid_table.num_rows + invalid_table.num_rows == raw_csv.num_rows

            # check if valid product count is correct
            assert (
                valid_table.to_pandas()["image"].replace("", np.nan).dropna().count()
                == valid_table.num_rows
            )

            # check that products in invalid set has no images
            assert (
                invalid_table.to_pandas()["image"].replace("", np.nan).dropna().count()
                == 0
            )
    cleanup()


def test_schema_validation():
    file_path = os.path.join(fixture_path, "wrong_schema.csv")
    with pytest.raises(Exception, match=r"Invalid scehma for input CSV file"):
        InputStreamReader(file_path).validate_schema()


def cleanup():
    for file in glob.glob(os.path.join(test_data_path, "*")):
        os.remove(file)
