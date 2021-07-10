drop table if exists dim_patients;
create table dim_patients
distkey(patient_sk)
as (    
    with vaccinations as
    (
        select * from staging_vaccinations
    ),

    patients as
    (
        select
            patient_sk,
            max(patient_id) as patient_id,
            max(patient_age)::integer as patient_age,
            max(patient_birth_date) as patient_birth_date,
            max(patient_biological_gender_enum) as patient_biological_gender_enum,
            max(patient_skin_color_code) as patient_skin_color_code,
            max(patient_skin_color_value) as patient_skin_color_value,
            max(patient_address_city_ibge_code) as patient_address_city_ibge_code,
            max(patient_address_city_name) as patient_address_city_name,
            max(patient_address_state_abbrev) as patient_address_state_abbrev,
            max(patient_address_country_code) as patient_address_country_code,
            max(patient_address_country_name) as patient_address_country_name,
            max(patient_address_postal_code) as patient_address_postal_code,
            max(patient_nationality_enum) as patient_nationality_enum,
            max(vaccination_category_code) as vaccination_category_code,
            max(vaccination_category_name) as vaccination_category_name,
            max(vaccination_subcategory_code) as vaccination_subcategory_code,
            max(vaccination_subcategory_name) as vaccination_subcategory_name
        from
            vaccinations
        group by
            1
    )

    select * from patients
)