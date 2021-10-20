# Extraction pipeline

> *Note:* This `README.md` was generated manually. Please modify it according to the new structure and contents.

## Overview

This modular pipeline reads and writes the data from the sources list to the blobs list.

## Pipeline inputs

### `Source Database`

|      |                    |
| ---- | ------------------ |
| Type | `spark.SparkJDBCDataSet` |
| Description | Input data from the Decameron Server |

### `params: string`

|      |                    |
| ---- | ------------------ |
| Type | `string` |
| Description | the name of the source ['hw', 'hh', , ,]|

## Pipeline outputs

### `Blob Storage database`

|      |                    |
| ---- | ------------------ |
| Type | `kedro.extras.datasets.pandas.ParquetDataSet` |
| Description | dataframe containing the dataset related with the namespace |
