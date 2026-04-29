-- Staging: cadastro de classes de fundos (Resolução CVM 175).
-- Normaliza CNPJ de inteiro para string formatada para join com informes.
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_cadastro_classes') }}
),

cleaned as (
    select
        -- Normaliza CNPJ inteiro para string com zeros à esquerda (14 dígitos)
        printf('%014d', cnpj_classe)          as cnpj_fundo_normalizado,
        denominacao_social                     as nome_fundo,
        tipo_classe                            as tipo_fundo,
        classificacao                          as classe,
        classificacao_anbima                   as classe_anbima,
        indicador_desempenho                   as benchmark_declarado,
        situacao,
        case
            when situacao = 'Em Funcionamento Normal' then true
            else false
        end                                    as fundo_ativo
    from source
    where cnpj_classe is not null
)

select * from cleaned