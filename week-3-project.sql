with events as ( 
    select
        event_id,
        session_id,
        event_timestamp as event_at,
        trim(parse_json(event_details):"recipe_id", '"') as recipe_id,
        trim(parse_json(event_details):"event", '"') as event_type
    from events.website_activity
    group by 1,2,3,4,5
),

sessions_events as (
    select
        session_id,
        min(event_at) as min_event_at,
        max(event_at) as max_event_at,
        datediff(second,min(event_at), max(event_at)) as session_duration,
        iff(count_if(event_type = 'view_recipe') = 0, null,
            round(count_if(event_type = 'search') / count_if(event_type = 'view_recipe'))) as searches_per_recipe_view
    from events
    group by 1    
),

recipe_views as (
    select
        recipe_id,
        date(event_at) as event_date,
        count(*) as total_views
    from events
    where recipe_id is not null
    group by 1,2
    qualify row_number() over (partition by event_date order by total_views desc) = 1
),
    
base as (
    select
        date(min_event_at) as event_date,
        count(session_id) as total_sessions,
        round(avg(datediff('sec', min_event_at, max_event_at))) as avg_session_duration_seconds,
        max(searches_per_recipe_view) as avg_searches_per_recipe_view,
        max(recipe_name) as favorite_recipe
    from sessions_events 
    inner join recipe_views on recipe_views.event_date = date(sessions_events.min_event_at) 
    inner join chefs.recipe using (recipe_id)
    group by 1 
    order by 1
)

select
    *
from base;
