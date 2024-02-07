## Work 2 Homework

Attengion: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homewokr. If your solution includes code that is not in file format, please include these directly in the README file of your repository.

For the homework, we'll be working with the green taxi dataser located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

`You may need to reference the link below to download via Python in Mage:`

https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/

## Assginment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to database (and Google Clod!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course
  - `BONUS`: load the final three months using a for loop and `pd.concat`

I use the following code

```Python
@data_loader
def load_data(*args, **kwargs):
        
    # Base URL for the files
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-"

    # Months of the files to download
    months = ['10', '11', '12']

    # Declare the datatypes of the columns
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }

    # Parsing the datae columns
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    # List to store the dataframes of each file
    dfs = []

    for month in months:
        # Construct the full URL
        url = f"{base_url}{month}.csv.gz"
        
        # Load the dataframe and append it to the list
        df = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)

    # Concatenate all the dataframes into one
    df_final = pd.concat(dfs, ignore_index=True)

    return df_final
```
