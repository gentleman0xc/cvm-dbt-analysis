-- Staging: limpa e padroniza os informes diários brutos da CVM.
-- Responsabilidade única: limpeza. Sem joins, sem métricas.
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_informes_diarios') }}
),

cleaned as (
    select
        dt_comptc                                    as data_competencia,
        cnpj_fundo_classe                            as cnpj_fundo,
        tp_fundo_classe                              as tipo_fundo,
        id_subclasse                                 as subclasse,
        coalesce(vl_quota, 0)                        as valor_cota,
        coalesce(vl_patrim_liq, 0)                   as patrimonio_liquido,
        coalesce(captc_dia, 0)                       as captacao_bruta,
        coalesce(resg_dia, 0)                        as resgate_bruto,
        coalesce(captc_dia, 0) - coalesce(resg_dia, 0) as captacao_liquida,
        coalesce(nr_cotst, 0)                        as num_cotistas
    from source
    where
        vl_quota > 0
        and vl_patrim_liq > 0
        and dt_comptc is not null
        and cnpj_fundo_classe is not null
)

select * from cleaned