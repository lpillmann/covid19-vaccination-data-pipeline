drop table if exists fact_vaccinations;
create table fact_vaccinations
distkey(vaccination_sk)
interleaved sortkey(patient_sk, vaccination_date, city_sk, vaccine_sk, facility_sk)
as (    
    with staging_vaccinations as
    (
        select * from staging_vaccinations
    ),

    selected_columns as
    (
        select
            vaccination_sk,
            patient_sk,
            facility_sk,
            vaccine_sk,
            city_sk,
            vaccination_date,
            1 as vaccinations_count
        from
            staging_vaccinations
    )

    select * from selected_columns
)
