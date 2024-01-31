# Module 1 Homework

## Docker & SQL

## Question 1. Knowing docker tags

Run the command to get information on Docket

```bash
docker --help
```
Now run the command to get help on the "docker build" command:

```bash
docker build --help
```

Do the same for "docker run".

Which tag has the followint text? - Automatically remove the container when it exists.

**Answer**: -rm

## Question 2. Understanding docker first run

Run docker with python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python moules that are installed (use pip list) What is the version of the package wheel?

**Answer**: 0.42.0

## Prepare Postgres

Run Postgress and load data as shown in the videos. We'll use the green taxi trips from September 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
```

Download this data and put it into Posgres (with jupyter notebooks or with a pipeline)

I use the pipeline using the following code

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

## Question 3. Count records

How many taxi trips were totally made in September 18th 2019?

Tip: started and finished on 2019-09-18

I use the following SQL query:

```SQL
SELECT COUNT(1)
FROM green_taxi_trips 
WHERE DATE(lpep_pickup_datetime) = '2019-09-18' 
	AND
	DATE(lpep_dropoff_datetime) = '2019-09-18';
```

**Answer** 15612

## Question 4 Longest trip for each day

Which was the pickup day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every trip on a single day, we only care about the trip with the longest distance.

I use the following SQL query:

```SQL
SELECT 
    *, 
    lpep_dropoff_datetime - lpep_pickup_datetime AS duracion
FROM 
    yellow_taxi_trips
ORDER BY 
    duracion DESC
LIMIT 1;
```

The result in lpep_pickup_datetime is "2019-09-26 08:58:52"

**Answer**: 2019-09-26

## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ingnoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

```SQL
SELECT DATE(lpep_pickup_datetime), 
	"Borough",
	COUNT(*) as conteo
FROM 
    yellow_taxi_trips t Join
	zones z on t."PULocationID" = z."LocationID"
WHERE
	Date(lpep_pickup_datetime) = '2019-09-18'
GROUP BY
	Date(lpep_pickup_datetime),
	"Borough"
ORDER BY 
    conteo DESC;
```

**Answer** "Brooklyn", "Manhattan", "Queens

## Question 6 Largest Tip

For the passengers picked up in September 2019 in the zone name Astoria, which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Here is the query

```SQL
SELECT "Borough",
	"Zone",
	"DOLocationID",
	"tip_amount"
FROM 
    yellow_taxi_trips t Join
	zones z on t."PULocationID" = z."LocationID"
WHERE
	"Zone" LIKE '%Astoria%'
ORDER BY 
    "tip_amount" DESC;
```

The ID is 132 which correspond to JFK Airport

**Answer**: JFK Airport

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform

In your VM on GCP/Latop/Github Codespace install Terraform. Copy the files from the course repo [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/Github/Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```bash
terraform apply
```

Paste the output of this command into the homework submission form.
