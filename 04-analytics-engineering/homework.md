## Module 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres - only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

### Question 1: 

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'**
You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video. 
- 'It's the same as running *dbt build*'
- It applies a _limit 100_ to all of our models
- It applies a _limit 100_ only to our staging models
- Nothing

Since we are setting the variable 'is_test_run' 'true', and is the same as as the default value, then, it's the same as running *dbt build*.

### Question 2: 

**What is the code that our CI job will run? Where is this code coming from?**  

- The code that has been merged into the main branch
- The code that is behind the creation object on the dbt_cloud_pr_ schema
- The code from any development branch that has been opened based on main
- The code from the development branch we are requesting to merge to main


### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.
Do not add a deduplication step. Run this models without limits (is_test_run: false).

Here is the code for the stg_fhv_tripdata

``` SQL
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
   where dispatching_base_num is not null
   AND EXTRACT(YEAR FROM pickup_datetime) = 2019 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num	', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num	", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

from tripdata
-- where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run the dbt model without limits (is_test_run: false).

Here is the code for the fact_fhv_trips

```SQL
{{
    config(
        materialized='table'
    )
}}

WITH fhv_tripdata AS (
    SELECT *,
        'Green' AS service_type
    FROM {{ ref('stg_fhv_tripdata') }}
    WHERE pickup_locationid >= 1 AND pickup_locationid <= 265
    AND dropoff_locationid >= 1 AND dropoff_locationid <= 265
),
dim_zones AS (
    SELECT * 
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT fhv_tripdata.tripid, 
    fhv_tripdata.vendorid,  
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime, 
FROM fhv_tripdata
INNER JOIN dim_zones AS pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones AS dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
```

- 12998722
- '22998722'
- 32998722
- 42998722

The answer is '22998722'

![Row count](Count of records.png)

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV
- Green
- Yellow
- FHV and Green


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw4

Deadline: 22 February (Thursday), 22:00 CET


## Solution (To be published after deadline)

* Video: 
* Answers:
  * Question 1: 
  * Question 2: 
  * Question 3: 
  * Question 4: 
