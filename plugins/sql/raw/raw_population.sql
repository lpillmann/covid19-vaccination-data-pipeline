create table if not exists raw_population (
    state text,
    state_ibge_code integer,
    city_ibge_code integer,
    city text,
    estimated_population integer
)
