{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select  

    service_type,
    trip_year,
    year_quarter, 

    -- Revenue calculation 
    sum(total_amount) as revenue_quarterly_total_amount,


    from trips_data
    group by 1,2,3