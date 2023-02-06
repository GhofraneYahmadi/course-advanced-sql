--cities CTE: get the cities while eliminating duplicated ones
with cities as (
    select 
        upper(trim(city_name))  as city_name,
        upper(trim(state_name)) as state_name,
        upper(trim(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities
    qualify row_number() over (partition by upper(trim(city_name)), upper(trim(state_abbr)) order by city_name) = 1

 )
 
--customer_geolocation CTE: get the customers info and the geo location by performing a join to the cities CTE
,customer_geolocation as (
    select            
        ca.customer_id,
        cd.first_name as customer_first_name,
        cd.last_name  as customer_last_name,
        cd.email      as customer_email,
        upper(trim(ca.customer_city)) as customer_city,
        upper(trim(ca.customer_state)) as customer_state,
        c.geo_location as customer_geo_location
    from vk_data.customers.customer_address as ca
    inner join vk_data.customers.customer_data as cd on ca.customer_id=cd.customer_id
    inner join cities as c on upper(trim(ca.customer_city)) = upper(c.city_name) and upper(trim(ca.customer_state)) = upper(c.state_abbr)
)

--suppliers_geolocation CTE: get the suppliers info and the geo location by performing a join to the cities CTE
,suppliers_geolocation as (
     select
        supplier_id,
        supplier_name,
        upper(trim(si.supplier_city))  as supplier_city,
        upper(trim(si.supplier_state)) as supplier_state,
        c.geo_location as supplier_geo_location
    from vk_data.suppliers.supplier_info as si
    inner join cities as c on upper(trim(si.supplier_city)) = c.city_name and upper(trim(si.supplier_state)) = c.state_abbr
)

--shipping_distance CTE: calculate Shipping distance in kilometers by using the st_distance() 
,shipping_distance as (
    select 
        cg.customer_id,
        cg.customer_first_name,
        cg.customer_last_name,
        cg.customer_email,
        sg.supplier_id,
        sg.supplier_name,
        st_distance(cg.customer_geo_location,sg.supplier_geo_location)/1000 as shipping_distance_in_kilometers
    from customer_geolocation as cg 
    cross join suppliers_geolocation as sg
    qualify row_number() over (partition by customer_id order by st_distance(cg.customer_geo_location,sg.supplier_geo_location) asc )=1
)

select 
    customer_id                     as "Customer ID",
    customer_first_name             as "Customer first name",
    customer_last_name              as "Customer last name",
    customer_email                  as "Customer email",
    supplier_id                     as "Supplier ID",
    supplier_name                   as "Supplier name",
    shipping_distance_in_kilometers as "Shipping distance in kilometers"
from shipping_distance
order by 2,3
