# Transformater

## Install

### build docker image
```
docker build -t transformater .
```

### run tests
```
docker run -it transformater pytest /tests
```

### run flake8
```
docker run -it transformater flake8 /transformater
```

### run script
```
docker run -it transformater python transform.py
```
or pass custom s3 path
```
docker run -it transformater python transform.py --s3_bucket=backmarket-data-jobs --s3_file_path=data/product_catalog.csv
```

## Notes

I chose to write tranformatter project around pyarrow library for 3 reasones:
1. support of parquest transofrmations
2. support for data streaming
3. interoperability for different read/write stream format support like csv, json,

Code implemented with Interoperability in mind splitting the logic in 4 main abstractions:

- FileDownloader
- InputStreamReader
- ProductDataCleaner
- OutputStreamWriter

FileDownloader - __download_file function could be extended to support other sources not just s3
InputStreamReader & OutputStreamWriter


### Interoperability