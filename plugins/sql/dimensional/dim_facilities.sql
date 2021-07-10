drop table if exists dim_facilities;
create table dim_facilities
distkey(facility_sk)
as (    
    with vaccinations as
    (
        select * from staging_vaccinations
    ),

    facilities as
    (
        select
            facility_sk,
            max(facility_code) as facility_code,
            max(facility_registration_name) as facility_registration_name,
            max(facility_fantasy_name) as facility_fantasy_name,
            max(facility_city_code) as facility_city_code,
            max(facility_city_name) as facility_city_name,
            max(facility_state_abbrev) as facility_state_abbrev
        from
            vaccinations
        group by
            1
    )

    select * from facilities
)