-- Enriquece a série de cotas com cadastro e calcula retorno diário.
-- Usa cadastro de classes (RCVM175) com normalização de CNPJ para join.
{{ config(materialized='table') }}

with cotas as (
    select
        *,
        -- Remove pontos e barra do CNPJ para normalizar: 23.502.688/0001-03 -> 00023502688000103
        regexp_replace(cnpj_fundo, '[./-]', '', 'g') as cnpj_normalizado
    from {{ ref('stg_informes_diarios') }}
),

cadastro as (
    select distinct on (cnpj_fundo_normalizado)
        cnpj_fundo_normalizado,
        nome_fundo,
        tipo_fundo,
        classe,
        classe_anbima,
        benchmark_declarado,
        fundo_ativo
    from {{ ref('stg_cadastro_classes') }}
    where fundo_ativo = true
),

joined as (
    select
        c.data_competencia,
        c.cnpj_fundo,
        f.nome_fundo,
        f.tipo_fundo,
        f.classe,
        f.classe_anbima,
        f.benchmark_declarado,
        c.valor_cota,
        c.patrimonio_liquido,
        c.captacao_liquida,
        c.num_cotistas,

        (c.valor_cota / lag(c.valor_cota) over (
            partition by c.cnpj_fundo
            order by c.data_competencia
        )) - 1                              as retorno_diario,

        count(*) over (
            partition by c.cnpj_fundo
        )                                   as total_dias_historico

    from cotas c
    inner join cadastro f on c.cnpj_normalizado = f.cnpj_fundo_normalizado
)

select * from joined
where retorno_diario is not null