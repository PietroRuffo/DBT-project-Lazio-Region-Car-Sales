with 

source as (

    select * from {{ source('staging', 'dev_gcs_to_bigquery_vehicle_lazio_to_bigquery_v1') }}

),

renamed as (

    select
        vehicle_type,
        destination,
        usage,
        city,
        automaker,
        fuel,
        euro_class ,
        co2_emission,
        vehicle_mass,
        registration_month,
        registration_year,
        count(*) as sale_cnt

    from source
    --filtering for private cars
    where vehicle_type = 'A' 
    and usage = 'Own'
    and destination = 'Car for Transporting People'
    and registration_year >= 2009 --only interested in 2009-2019 period
    and  euro_class in ('0','1','2','3','4','5','6')
    group by 
    vehicle_type,
        destination,
        usage,
        city,
        automaker,
        fuel,
        euro_class ,
        co2_emission,
        vehicle_mass,
        registration_month,
        registration_year
    

)

select * from renamed
