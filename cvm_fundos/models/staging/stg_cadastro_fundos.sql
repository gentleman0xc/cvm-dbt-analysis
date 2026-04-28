-- Staging: limpa e padroniza o cadastro de fundos da CVM.
-- Responsabilidade única: limpeza. Sem joins, sem métricas.
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_cadastro_fundos') }}
),

cleaned as (
    select
        cnpj_fundo,
        trim(denom_social)                    as nome_fundo,
        dt_reg                                as data_registro,
        sit                                   as situacao,
        tp_fundo                              as tipo_fundo,
        coalesce(classe, 'Não Classificado')  as classe,
        rentab_fundo                          as benchmark_declarado,
        case
            when sit = 'EM FUNCIONAMENTO NORMAL' then true
            else false
        end                                   as fundo_ativo
    from source
    where cnpj_fundo is not null
)

select * from cleaned