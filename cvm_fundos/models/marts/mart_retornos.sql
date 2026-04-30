-- Métricas financeiras finais por fundo.
-- Sharpe Ratio, Drawdown Máximo, Retorno Acumulado.
-- Selic ~13.75% a.a. (abril/2026) usada como taxa livre de risco.
{{ config(materialized='table') }}

with historico as (
    select * from {{ ref('int_cotas_historico') }}
    where total_dias_historico >= 60  -- mínimo 60 dias para ter relevância
),

com_acumulado as (
    select
        *,
        -- Retorno acumulado: produto dos retornos diários
        exp(sum(ln(1 + retorno_diario)) over (
            partition by cnpj_fundo
            order by data_competencia
            rows between unbounded preceding and current row
        )) - 1                              as retorno_acumulado
    from historico
),

com_drawdown as (
    select
        *,
        -- Drawdown: queda em relação ao pico histórico até aquele dia
        retorno_acumulado - max(retorno_acumulado) over (
            partition by cnpj_fundo
            order by data_competencia
            rows between unbounded preceding and current row
        )                                   as drawdown_pct
    from com_acumulado
),

-- PL e cotistas mais recentes por fundo
ultimos as (
    select distinct on (cnpj_fundo)
        cnpj_fundo,
        patrimonio_liquido          as pl_atual,
        num_cotistas                as cotistas_atual,
        retorno_acumulado           as retorno_acumulado_total
    from com_drawdown
    order by cnpj_fundo, data_competencia desc
),

metricas_fundo as (
    select
        h.cnpj_fundo,
        h.nome_fundo,
        h.tipo_fundo,
        h.classe,
        h.classe_anbima,
        -- Anualização: multiplica média diária por 252 pregões
        avg(h.retorno_diario) * 252                     as retorno_anualizado,
        stddev(h.retorno_diario) * sqrt(252)            as volatilidade_anualizada,
        -- Sharpe: (retorno - taxa livre de risco) / volatilidade
        (avg(h.retorno_diario) * 252 - 0.1375)
            / nullif(stddev(h.retorno_diario) * sqrt(252), 0)
                                                        as sharpe_ratio,
        min(h.drawdown_pct)                             as drawdown_maximo,
        sum(h.captacao_liquida)                         as captacao_liquida_total,
        count(distinct h.data_competencia)              as dias_historico,
        min(h.data_competencia)                         as data_inicio,
        max(h.data_competencia)                         as data_fim
    from com_drawdown h
    group by 1, 2, 3, 4, 5
)

select
    m.*,
    u.pl_atual,
    u.cotistas_atual,
    u.retorno_acumulado_total,
    case
        when m.sharpe_ratio > 1.0 then 'Excelente'
        when m.sharpe_ratio > 0.5 then 'Bom'
        when m.sharpe_ratio > 0.0 then 'Neutro'
        else 'Ruim'
    end                                                 as classificacao_sharpe
from metricas_fundo m
join ultimos u using (cnpj_fundo)
where
    m.volatilidade_anualizada > 0
    and u.pl_atual >= 1000000  -- PL mínimo R$ 1M
order by m.sharpe_ratio desc nulls last