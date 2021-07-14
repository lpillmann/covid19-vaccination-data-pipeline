drop table if exists dim_vaccines;
create table dim_vaccines
diststyle all
sortkey(vaccine_sk)
as (    
    with vaccinations as
    (
        select * from staging_vaccinations
    ),

    vaccines as
    (
        select
            vaccine_sk,
            max(vaccination_dose_description) as vaccination_dose_description,
            max(vaccine_type_code) as vaccine_type_code,
            max(vaccine_type_name) as vaccine_type_name,
            max(vaccine_batch_code) as vaccine_batch_code,
            max(vaccine_manufacturer_name) as vaccine_manufacturer_name,
            max(vaccine_manufacturer_reference_code) as vaccine_manufacturer_reference_code
        from
            vaccinations
        group by
            1
    )

    select * from vaccines
)