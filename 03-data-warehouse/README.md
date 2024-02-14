# Module 3 Homework

Attention: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

Important Note:

For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

If you are using orchestration as Mage, Airflow or Project do not load the data into Big Query using the orchestrator.

Load the files into a bucket.

I use the following Mage code.

For data loading:

```Python
def load_data_from_api(*args, **kwargs):

    #Specifying the type of service
    service = 'green'

    #Specifying the year
    year = '2022'

    # Printing what we are loading
    print(f'loading {service} taxi data for the year {year}\n')

    #Creating the data frame for all the information
    data_frames = []

    #Looping trough all the files to load them into the data frame.
    for i in range(12):
        #Formatting the month
        month = f"{i+1:02d}"
        #Creating the file name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        #Creating the URL
        request_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'
        print(f'request url: {request_url}')
        #Loading the PARQUET file
        try:
            response = requests.get(request_url)
            response.raise_for_status()  # Raises HTTPError for bad requests
            data = io.BytesIO(response.content)
            df = pq.read_table(data).to_pandas()
            data_frames.append(df)
            print(f"Parquet loaded: {file_name}")
        except requests.HTTPError as e:
            print(f"HTTP Error: {e}")
            # Optionally, handle the error (e.g., by breaking the loop or re-trying)

    # Concatenate all dataframes
    combined_df = pd.concat(data_frames, ignore_index=True)
    return combined_df
```

Then the information is migrated to a bucket in Google Cloud Storage.

```Python
@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'use_your_bucket_name'
    object_key = 'green_taxi_2022.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
```

NOTE: You will need to use the PARQUET option files when creating an External Table

SETUP:
Create an external table using the Green Taxi Records Data for 2022. The external table was creating inside BigQuery with the ADD botton looking for the information inside buckets in Google Cloud Storage.

Create a  table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
The table was created with the following SQL query:

```SQL
CREATE OR REPLACE TABLE project.table.green_taxi_2022_table AS
SELECT VendorID,
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS lpep_dropoff_datetime,
  store_and_fwd_flag,
  RatecodeID,
  PULocationID,
  DOLocationID,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM project.table.green_taxi_2022;
```

# Question 1:

Question 1: What is the count of records for the 2022 Green Taxi Data?

- 65,623,481
- `840,402`
- 1,936,423
- 253,647

There are several ways to obtain this answer. You can take it from Mage when loading the information. You can look in the `Details` tab in BigQuery under the section `Storage Information` in number of rows. Finally, you can obtain the information by running the following query:

```SQL
SELECT COUNT(*) AS total_rows
FROM `project.table.green_taxi_2022_table`;
```

# Question 2:

Write a query to count the distinct number of PULocationIDS for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- `0 MB for the External Table and 6.41MB for the Materialized Table`
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

The Query for the external data is:

```SQL
SELECT COUNT(DISTINCT PULocationID) AS unique_PULocationIDs
FROM `project.table.green_taxi_2022`;
```

![Data Consumption External Table](question_2_external_table.png)

The Query for the table in BigQuery:

```SQL
SELECT COUNT(DISTINCT PULocationID) AS unique_PULocationIDs
FROM `project.table.green_taxi_2022_table`
```

![Data Consumption Native Table](question_2_native_table.png)

The result is that the External Table will cost 0 MB while the Materialized Table will take 6.41 MB

# Question 3:

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- `1,622`

To answer the question I use the following query:

```SQL
SELECT COUNT(fare_amount) AS zero_fare_amount
FROM `eminent-torch-412600.ny_taxi.green_taxi_2022_table`
WHERE fare_amount = 0;
```

The query through 1,622 as the answer.

# Question 4:

What is the best strategy to make an optimized table in BigQuery if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- `Partition by lpep_pickup_datetime Cluster on PUlocationID`
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

Partitioning the table by `lpep_pickup_datetime` and clustering by `PULocationID` is an effective strategy for optimizing query performance and cost in BigQuery for several reasons. Partitioning by `lpep_pickup_datetime` organizes the data into manageable, discrete segments based on time, which aligns perfectly with queries that filter by pickup date. This approach significantly reduces the volume of data scanned during these queries, leading to faster execution times and lower costs, as BigQuery only processes the relevant partitions that match the query's date filter criteria. Clustering by `PULocationID` within those partitions further optimizes query performance by arranging data based on pickup locations. This means that when queries also include conditions on `PULocationID` (e.g., ordering or filtering), BigQuery can quickly locate and access the relevant rows within each partition, further reducing the amount of data scanned and improving query efficiency. Together, partitioning and clustering optimize data storage and access patterns, making the table structure highly efficient for the specific query patterns described, resulting in performance improvements and cost savings.

The query for creating the new table is:

```SQL
CREATE OR REPLACE TABLE `project.table.green_taxi_data_optimized`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT *
FROM `project.table.green_taxi_2022_table`;
```

# Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- `12.82 MB for non-partitioned table and 1.12 MB for the partitioned table`
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

The Query for the external data is:

```SQL
SELECT DISTINCT(PULocationID)
FROM `project.table.green_taxi_2022_table`
WHERE lpep_pickup_datetime >= '2022-06-01 00:00:00'
AND lpep_pickup_datetime <= '2022-06-30 23:59:59';
```

![Data Consumption Non Partitioned Table](question5_non_partitioned_table.png)

The Query for the table in BigQuery:

```SQL
SELECT DISTINCT(PULocationID)
FROM `project.table.green_taxi_2022_partitioned_cluster_table`
WHERE lpep_pickup_datetime >= '2022-06-01 00:00:00'
AND lpep_pickup_datetime <= '2022-06-30 23:59:59';
```

![Data Consumption Partitioned and Clustered Table](question5_partitioned_table.png)

The result is that the Non Partitioned Table will cost 12.82 MB while the Partitioned and Clustered Table will take 1.12 MB

# Question 6:

Where is the data stored in the External Table you created?

- Big Query
- `GCP Bucket`
- Big Table
- Container Registry

When you create an external table in BigQuery that references data in a Google Cloud Storage (GCP) bucket, the data itself remains stored in the Cloud Storage bucket and is not physically imported or stored within BigQuery's storage system. BigQuery accesses the data directly from Cloud Storage when you query the external table. 

# Question 7:

It is the best practice in BigQuery to always cluster your data?

- True
- `False`

False, it is not always the best practice to cluster your data in BigQuery. While clustering can significantly improve query performance and reduce costs by organizing data based on the values of specific columns, it is most beneficial when your queries frequently filter or aggregate data on the clustered columns. If your dataset is small, your query patterns do not benefit from clustering, or you don't frequently access the data, the overhead of maintaining clustered tables may not be justified. Therefore, whether to use clustering should be determined based on your specific data access patterns, the size of your datasets, and your query requirements.

# (Bonus: Not worth points) Question 8:

No Points: Write a `SELECT count(*)`query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

When you execute a `SELECT count(*)` query against a materialized view in BigQuery, the query processing system may not need to scan any additional bytes of data beyond what is stored in the materialized view itself, especially if the count is maintained as part of the materialized view's metadata. 
