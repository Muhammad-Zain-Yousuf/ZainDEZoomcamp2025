## Homework 1
### Question 1:
docker run -it --entrypoint bash python:3.12.8
(Bash) pip --versions

### Question 2:
db:5432

### Question 3:
**Up to 1 Mile:**
```
select count(*) 
from green_taxi_data
where trip_distance <= 1
```
**In between 1 (exclusive) and 3 miles (inclusive)**
```
select count(*) 
from green_taxi_data
where trip_distance > 1 and trip_distance <= 3
```
**In between 3 (exclusive) and 7 miles (inclusive)**
```
select count(*) 
from green_taxi_data
where trip_distance > 3 and trip_distance <= 7
```
**In between 7 (exclusive) and 10 miles (inclusive)**
```
select count(*) 
from green_taxi_data
where trip_distance > 7 and trip_distance <= 10
```
**Over 10 Miles**
```
select count(*) 
from green_taxi_data
where trip_distance > 10 
```
### Question 4:
```
select date(lpep_pickup_datetime) as pickup_date, max(trip_distance) as max_trip_per_day
from green_taxi_data
group by date(lpep_pickup_datetime)
order by max_trip_per_day desc
limit 1
```
### Question 5
```
select ZL."Zone"
from green_taxi_data GT
inner join taxi_zone_lookup ZL on ZL."LocationID" = GT."PULocationID" 
where GT.lpep_pickup_datetime::date = '2019-10-18'
group by ZL."Zone"
having sum(GT."total_amount") > 13000
```
### Question 6
```
select ZL2."Zone"
from (
	select *
	from green_taxi_data GT
	inner join taxi_zone_lookup ZL on ZL."LocationID" = GT."PULocationID" 
	where ZL."Zone" = 'East Harlem North' and GT.lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
	order by GT."tip_amount" desc
	limit 1
) AS T1
inner join taxi_zone_lookup ZL2 on ZL2."LocationID" = T1."DOLocationID"
```

### Question 7
```
terraform init
terraform apply -auto-approve
terraform destroy
```
