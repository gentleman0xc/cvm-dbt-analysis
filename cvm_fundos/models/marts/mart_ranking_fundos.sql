-- Ranking de fundos por Sharpe Ratio dentro de cada classe.
-- Filtra fundos com histórico mínimo e PL relevante.
{{ config(materialized='table') }}

with base as (
    select * from {{ ref('mart_retornos') }}
    where
        dias_historico >= 60
        and pl_atual >= 1000000
        and sharpe_ratio is not null
        and classe is not null
),

ranked as (
    select
        *,
        -- Ranking dentro de cada classe por Sharpe
        row_number() over (
            partition by classe
            order by sharpe_ratio desc
        )                               as rank_na_classe,

        -- Ranking geral por Sharpe
        row_number() over (
            order by sharpe_ratio desc
        )                               as rank_geral
    from base
)

select
    rank_geral,
    rank_na_classe,
    cnpj_fundo,
    nome_fundo,
    classe,
    classe_anbima,
    round(sharpe_ratio, 2)                  as sharpe_ratio,
    round(retorno_anualizado * 100, 2)      as retorno_anualizado_pct,
    round(volatilidade_anualizada * 100, 2) as volatilidade_anualizada_pct,
    round(drawdown_maximo * 100, 2)         as drawdown_maximo_pct,
    round(retorno_acumulado_total * 100, 2) as retorno_acumulado_pct,
    round(pl_atual / 1e6, 2)               as pl_milhoes,
    cotistas_atual,
    dias_historico,
    classificacao_sharpe
from ranked
order by rank_geral