# Question 1
```
select count(*)
from 'de_zoomcamp.yellow_tripdata_2024';
```
# Question 2
```
select count(distinct PULocationID)
from 'de_zoomcamp.yellow_tripdata_2024';

select count(distinct PULocationID)
from 'de_zoomcamp.yellow_tripdata_2024_ext';
```
# Question 3
```
select PULocationID
from 'de_zoomcamp.yellow_tripdata_2024';

select PULocationID, DOLocationID
from 'de_zoomcamp.yellow_tripdata_2024';
```
# Question 4
```
select count(*)
from 'de_zoomcamp.yellow_tripdata_2024'
where fare_amount = 0;
```
# Question 5
```
create table
'zain-de-zoomcamp-2025.de_zoomcamp.yellow_tripdata_2024_clustered'
partition by
date(tpep_dropoff_datetime)
cluster by VendorID as
select *
from 'zain-de-zoomcamp-2025.de_zoomcamp.yellow_tripdata_2024';
```
# Question 6
```
select distinct VendorID
from 'de_zoomcamp.yellow_tripdata_2024'
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

select distinct VendorID
from 'de_zoomcamp.yellow_tripdata_2024_clustered'
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
```
