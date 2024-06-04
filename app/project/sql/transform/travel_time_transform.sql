{% set config = {
    "extract_type":"incremental",
    "incremental_column": "load_timestamp",
    "source_table_name":"travel_time_staging"
} %}

select
 load_id AS travel_time_id,
 case when cast(location_id as VARCHAR) = '40.8871438,-74.0410865' then 'Hackensack'
 when cast(location_id as VARCHAR) = '40.7433066,-74.0323752' then 'Hoboken'
 when cast(location_id as VARCHAR) = '41.0534302,-73.5387341' then 'Stamford'
 end as starting_city,
 'New York' AS destination_city,
 travel_time / 60 as travel_time_minutes,
 case
    when EXTRACT(hour from load_timestamp) >= 5 and EXTRACT(hour from load_timestamp) < 12 then 'Morning'
    when EXTRACT(hour from load_timestamp) >= 12 AND EXTRACT(hour from load_timestamp) < 17 then 'Afternoon'
    when EXTRACT(hour from load_timestamp) >= 17 AND EXTRACT(hour from load_timestamp) < 21 then 'Evening'
    else 'Night'
 end as day_part,
 case
    when EXTRACT(hour from load_timestamp) >= 7 AND EXTRACT(hour from load_timestamp) < 10 then 'TRUE'
    when EXTRACT(hour from load_timestamp) >= 15 AND EXTRACT(hour from load_timestamp) < 19 then 'TRUE'
    else 'FALSE'
 end as rush_hour_indicator,
 load_timestamp,
 'incremental' as load_type
from
 {{config["source_table_name"]}}
{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}
