FROM python:3.8
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .


# build docker
# docker build -t transformater .

# run tests
# docker run -it transformater pytest /tests

# run flake8
# docker run -it transformater flake8 /transformater

# run script
# docker run -it transformater python transform.py --s3_bucket=backmarket-data-jobs --s3_file_path=data/product_catalog.csv
# docker run -it transformater python transform.py