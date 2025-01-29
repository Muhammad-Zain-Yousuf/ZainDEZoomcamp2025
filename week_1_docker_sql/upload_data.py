import pandas as pd
import sqlalchemy
from time import time

engine = sqlalchemy.create_engine("postgresql://root:root@pg-database:5432/ny_taxi")

df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize=100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists='replace')
df.to_sql(name="yellow_taxi_data", con=engine, if_exists='append')

while True:
    start = time()
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name="yellow_taxi_data", con=engine, if_exists='append')

    print(f"Inserted another chunk..., took {time() - start} seconds")
