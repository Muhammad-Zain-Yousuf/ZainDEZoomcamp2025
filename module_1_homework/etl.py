import pandas as pd
import sqlalchemy
from time import time

engine = sqlalchemy.create_engine("postgresql://root:root@localhost:5433/ny_taxi")

df_iter = pd.read_csv("taxi_zone_lookup.csv", iterator=True, chunksize=100000)

df = next(df_iter)


# df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
# df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


df.head(n=0).to_sql(name="taxi_zone_lookup", con=engine, if_exists='replace')
df.to_sql(name="taxi_zone_lookup", con=engine, if_exists='append')

while True:
    start = time()
    df = next(df_iter)
    
    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name="taxi_zone_lookup", con=engine, if_exists='append')

    print(f"Inserted another chunk..., took {time() - start} seconds")
