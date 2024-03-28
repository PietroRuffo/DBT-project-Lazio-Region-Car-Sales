{{
    config(
        materialized='table',
        partition_by={
      "field": "registration_year",
      "data_type": "int64",
      "range": {
        "start": 2009,
        "end": 2019,
        "interval": 1
      }
    }
    )
}}

select vehicle_type,
        destination,
        usage,
        city,
        automaker,
        fuel,
        euro_class,
        co2_emission,
        vehicle_mass,
        registration_month,
        registration_year,
        sale_cnt
from {{ ref('stg_staging__dev_gcs_to_bigquery_vehicle_lazio_to_bigquery_v1') }}