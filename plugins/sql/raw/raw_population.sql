drop table if exists raw_population;
create table raw_population (
    state text,
    state_ibge_code integer,
    city_ibge_code integer,
    city text,
    estimated_population integer
)
