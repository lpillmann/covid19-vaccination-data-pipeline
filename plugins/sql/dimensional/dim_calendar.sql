drop table if exists dim_calendar;
create table dim_calendar
sortkey(full_date)
as (    
    select
        vaccination_date as full_date,
        extract(day from vaccination_date) as day,
        extract(week from vaccination_date) as week,
        extract(month from vaccination_date) as month,
        extract(year from vaccination_date) as year,
        extract(weekday from vaccination_date) as weekday
    from
        fact_vaccinations
    group by 1
)
