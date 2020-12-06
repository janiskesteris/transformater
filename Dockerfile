FROM python:3.8
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .


# run tests
# docker run -it bm_product_catalog_etl pytest /tests

# run script
# docker run -it bm_product_catalog_etl python main.py --s3_bucket=backmarket-data-jobs --s3_file_path=data/product_catalog.csv
# docker run -it bm_product_catalog_etl python main.py