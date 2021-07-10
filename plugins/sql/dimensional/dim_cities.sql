drop table if exists dim_cities;
create table dim_cities
diststyle all
sortkey(city_sk)
as (    
    with population as
    (
        select * from raw_population
    ),

    create_cropped_city_code_column as
    (
        /* This is done because city codes in vaccination table have only 6 characters */
        select
            *,
            substring(city_ibge_code, 0, 7) as cropped_city_ibge_code
        from
            population
    ),

    create_surrogate_key as
    (
        select
            md5(
            coalesce(cropped_city_ibge_code::text, '') 
            ) as city_sk,
            *
        from
            create_cropped_city_code_column
    )

    select * from create_surrogate_key
)