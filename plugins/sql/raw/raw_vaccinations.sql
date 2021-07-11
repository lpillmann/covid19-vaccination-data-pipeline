drop table if exists raw_vaccinations;
create table raw_vaccinations (
    document_id text,
    paciente_id text,
    paciente_idade integer,
    paciente_dataNascimento text,
    paciente_enumSexoBiologico text,
    paciente_racaCor_codigo text,
    paciente_racaCor_valor text,
    paciente_endereco_coIbgeMunicipio text,
    paciente_endereco_coPais text,
    paciente_endereco_nmMunicipio text,
    paciente_endereco_nmPais text,
    paciente_endereco_uf text,
    paciente_endereco_cep text,
    paciente_nacionalidade_enumNacionalidade text,
    estabelecimento_valor text,
    estabelecimento_razaoSocial text,
    estalecimento_noFantasia text,
    estabelecimento_municipio_codigo text,
    estabelecimento_municipio_nome text,
    estabelecimento_uf text,
    vacina_grupoAtendimento_codigo text,  -- patient attribute
    vacina_grupoAtendimento_nome text,  -- patient attribute
    vacina_categoria_codigo text,  -- patient attribute
    vacina_categoria_nome text,  -- patient attribute
    vacina_lote text,
    vacina_fabricante_nome text,
    vacina_fabricante_referencia text,
    vacina_dataAplicacao text,   -- vaccination (event) attribute
    vacina_descricao_dose text,
    vacina_codigo text,
    vacina_nome text,
    sistema_origem text,  -- raw data attribute (when data was imported to the unified data system)
    id_sistema_origem text,  -- raw data attribute (when data was imported to the unified data system)
    data_importacao_rnds text,  -- raw data attribute (when data was imported to the unified data system)
    redshift text,  -- raw data attribute (when data was imported to the unified data system)
    timestamp text, 
    version text,
    year_month text
)