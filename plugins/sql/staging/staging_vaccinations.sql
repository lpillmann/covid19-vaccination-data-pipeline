drop table if exists staging_vaccinations;
create table staging_vaccinations as (
    with translate_column_names_and_convert_types as
    (
        select
            document_id as elasticsearch_document_id,
            paciente_id as patient_id,
            paciente_idade::integer as patient_age,
            paciente_dataNascimento::date as patient_birth_date,
            paciente_enumSexoBiologico as patient_biological_gender_enum,
            paciente_racaCor_codigo as patient_skin_color_code,
            paciente_racaCor_valor as patient_skin_color_value,
            paciente_endereco_coIbgeMunicipio as patient_address_city_ibge_code,
            paciente_endereco_nmMunicipio as patient_address_city_name,
            paciente_endereco_uf as patient_address_state_abbrev,
            paciente_endereco_coPais as patient_address_country_code,
            paciente_endereco_nmPais as patient_address_country_name,
            paciente_endereco_cep as patient_address_postal_code,
            paciente_nacionalidade_enumNacionalidade as patient_nationality_enum,
            estabelecimento_valor::integer as facility_code,
            estabelecimento_razaoSocial as facility_registration_name,
            estalecimento_noFantasia as facility_fantasy_name,
            estabelecimento_municipio_codigo::integer as facility_city_code,
            estabelecimento_municipio_nome as facility_city_name,
            estabelecimento_uf as facility_state_abbrev,
            vacina_categoria_codigo as vaccination_category_code,
            vacina_categoria_nome as vaccination_category_name,
            vacina_grupoAtendimento_codigo as vaccination_subcategory_code,
            vacina_grupoAtendimento_nome as vaccination_subcategory_name,
            vacina_lote as vaccine_batch_code,
            vacina_fabricante_nome as vaccine_manufacturer_name,
            vacina_fabricante_referencia as vaccine_manufacturer_reference_code,
            vacina_dataAplicacao::date as vaccination_date,
            vacina_descricao_dose as vaccination_dose_description,
            vacina_codigo::integer as vaccine_type_code,
            vacina_nome as vaccine_type_name,
            id_sistema_origem as source_system_code,
            sistema_origem as source_system_name,
            data_importacao_rnds::date as data_loaded_from_rnds_date,
            redshift as redshift,
            timestamp::timestamp as elasticsearch_timestamp,
            version as elasticsearch_version,
            year_month::date as year_month
        from
            raw_vaccinations
    ),

    create_surrogate_keys as
    (
        /* Create uniform identifiers to be shared among fact and dimension tables downstream */
        select
            md5(
                coalesce(elasticsearch_document_id, '') 
                || coalesce(patient_id::text, '')
                || coalesce(vaccination_date::text, '')
            ) as vaccination_sk,

            md5(
                coalesce(patient_id::text, '')
            ) as patient_sk,

            md5(
                coalesce(facility_code::text, '')
            ) as facility_sk,

            md5(
                coalesce(vaccine_type_code::text, '') 
                || coalesce(vaccine_batch_code, '')
                || coalesce(vaccination_dose_description, '')
            ) as vaccine_sk,

            md5(
                coalesce(patient_address_city_ibge_code, '') 
            ) as city_sk,

            translate_column_names_and_convert_types.*
        from
            translate_column_names_and_convert_types
    )

    select * from create_surrogate_keys

)