with active_customers as (
    select 
        customer_id
        ,count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),    

chicago_city as ( 
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),

gary_city as (
     select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
),

cities as (
    select
        trim(state_abbr) as state_abbr
        , trim(city_name) as city_name
        , geo_location as geo_location

    from vk_data.resources.us_cities 
    where 
        ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%') and state_abbr = 'KY')
        or (state_abbr = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
        or (state_abbr = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%')
),

base as (
    select 
        first_name || ' ' || last_name as customer_name
        , customer_address.customer_city
        , customer_address.customer_state
        , active_customers.food_pref_count
        , (st_distance(cities.geo_location, chicago_city.geo_location) / 1609)::int as chicago_distance_miles
        , (st_distance(cities.geo_location, gary_city.geo_location) / 1609)::int as gary_distance_miles
    from vk_data.customers.customer_address 
    inner join vk_data.customers.customer_data on customer_address.customer_id = customer_data.customer_id
    left join cities on upper(trim(customer_address.customer_state)) = upper(cities.state_abbr) and lower(trim(customer_address.customer_city))=lower(trim(cities.city_name))
    inner join active_customers on customer_data.customer_id = active_customers.customer_id
    cross join chicago_city
    cross join gary_city
    where ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%') and customer_state = 'KY') 
    or (customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
    or (customer_state = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%')
)

select 
    *
from base;
