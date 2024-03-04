## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

The version of spark version can be obtained with the following code

```Python
print(spark.version)
```

The result is `3.5.0`

### Question 2: 

**FHV October 2019**

First let's download the file with wget and unzip it with gunzip

```bash
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz
!gunzip -c fhv_tripdata_2019-10.csv.gz > fhv_tripdata_2019-10.csv
```

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

```Python
df = spark.read \
    .option("header", "true") \
    .csv('fhv_tripdata_2019-10.csv')
```

Defining the schema, first we need to look at the columns with and using pandas

```Python
df.schema
df_pandas.dtypes
```

The schema should be

```Python
schema = types.StructType([
    types.StructField('dispatching_base_num', types.IntegerType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PUlocationID', types.IntegerType(), True), 
    types.StructField('DOlocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.IntegerType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)])
```

Repartition the Dataframe to 6 partitions and save it to parquet. The repartition can be made with and then it can be saved with

```Python
df = df.repartition(6)
df.write.parquet('fhv/2019/10/')
```

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

Then the size can be obtained with

```bash
ls -lh fhv/2019/10/
```

- 1MB
- `6MB`
- 25MB
- 87MB

Each file has a size of 5.6 MB

### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

To do this, first we need to create the temporary table with

```PYTHON
df.registerTempTable('fhv_2019_10')
```

Then we can use SQL in SPARK to count the trips on October 15th.

```Python
spark.sql("""
SELECT 
    count(1)
FROM fhv_2019_10 
WHERE DATE(pickup_datetime) = '2019-10-15' 
""").show()
```

It results `62,610` trips on that day.

- 108,164
- 12,856
- 452,470
- `62,610`

> [!IMPORTANT]
> Be aware of columns order when defining schema

### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

To determine the longest trip we can use the following code 

```Python
spark.sql("""
SELECT
  *,
  (UNIX_TIMESTAMP(dropOff_datetime) - UNIX_TIMESTAMP(pickup_datetime)) / 3600 AS duracion_horas
FROM
  fhv_2019_10
ORDER BY
  duracion_horas DESC
LIMIT 1
""").show()
```

- `631,152.50 Hours`
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours

The result is `631153.5` because there was an error in the dropoff_datetime, the year is 2091 not 2019

### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- `4040`
- 8080

The port is http://localhost:4040/

### Question 6: 

**Least frequent pickup location zone**

We can determine which of the zones if the least frequent pickup location zone by grouping by PUlocationID and then ordering the count by ascending order with the following code:


Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- `Jamaica Bay`
- Union Sq
- Crown Heights North

The least zone is 2 which correspond to Jamaica Bay.
