# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

![Question 0](Question_0.png)

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

```SQL
CREATE MATERIALIZED VIEW taxi_trips_average AS
SELECT
  tz1.zone AS pickup_zone,
  tz2.zone AS dropoff_zone,
  AVG(td.tpep_dropoff_datetime-td.tpep_pickup_datetime) AS avg_trip_time,
  MIN(td.tpep_dropoff_datetime-td.tpep_pickup_datetime) AS min_trip_time,
  MAX(td.tpep_dropoff_datetime-td.tpep_pickup_datetime) AS max_trip_time
FROM  
  trip_data td 
JOIN
  taxi_zone tz1 ON td.pulocationid = tz1.location_id
JOIN
  taxi_zone tz2 ON td.dolocationid = tz2.location_id
GROUP BY
  tz1.zone, tz2.zone;
```

Note: Do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:

```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

```SQL
SELECT
  *
FROM 
  taxi_trips_average
ORDER BY  
  avg_trip_time DESC
LIMIT 10;
```

![Question 1](Question_1.png)

Options:
1. `Yorkville East, Steinway`
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

The query for the materialized view is the following

```SQL
CREATE MATERIALIZED VIEW taxi_trips_average_count AS
SELECT
  tz1.zone AS pickup_zone,
  tz2.zone AS dropoff_zone,
  COUNT(*) as number_trips,
  AVG(td.tpep_dropoff_datetime-td.tpep_pickup_datetime) AS avg_trip_time,
  MIN(td.tpep_dropoff_datetime-td.tpep_pickup_datetime) AS min_trip_time,
  MAX(td.tpep_dropoff_datetime-td.tpep_pickup_datetime) AS max_trip_time
FROM  
  trip_data td 
JOIN
  taxi_zone tz1 ON td.pulocationid = tz1.location_id
JOIN
  taxi_zone tz2 ON td.dolocationid = tz2.location_id
GROUP BY
  tz1.zone, tz2.zone;
```

Then we can query with 

```SQL
SELECT *
FROM taxi_trips_average_count
WHERE "pickup_zone" = 'Yorkville East'
AND "dropoff_zone" = 'Steinway';
```

Options:
1. 5
2. 3
3. 10
4. `1`

![Question 2](Question_2.png)

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

The query is

```SQL
SELECT 
  tz.zone as pickup_zone,
  count(*) as number_trips
FROM 
  trip_data td
JOIN 
  taxi_zone tz ON td.pulocationid = tz.location_id
WHERE 
  td.tpep_pickup_datetime >= (SELECT MAX(tpep_pickup_datetime)-interval '17 hours' FROM trip_data)
  AND
  td.tpep_pickup_datetime <= (SELECT MAX(tpep_pickup_datetime) FROM trip_data)
GROUP BY
  pickup_zone
ORDER BY
  number_trips DESC
LIMIT 3;
```

Options:
1. Clinton East, Upper East Side North, Penn Station
2. `LaGuardia Airport, Lincoln Square East, JFK Airport`
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

![Question 3](Question_3.png)
