-- Databricks notebook source
with tb_pedido as (

  select
    t1.idPedido,
    t2.idVendedor,
    t1.descSituacao,
    t1.dtPedido,
    t1.dtAprovado,
    t1.dtEntregue,
    t1.dtEstimativaEntrega,
    sum(vlFrete) as totalFrete 

  from silver.olist.pedido as t1

  left join silver.olist.item_pedido as t2
  on t1.idPedido = t2.idPedido

  where dtPedido < '2018-01-01'
  and dtPedido >= add_months('2018-01-01', -6)
  and idVendedor is not null

  group by 
    t1.idPedido,
    t2.idVendedor,
    t1.descSituacao,
    t1.dtPedido,
    t1.dtAprovado,
    t1.dtEntregue,
    t1.dtEstimativaEntrega
)

select

  '2018-01-01' as dtReference,
  date(now()) as dtIngestion,
  idVendedor,
  count(distinct case when date(coalesce(dtEntregue, '2018-01-01')) > date(dtEstimativaEntrega) then idPedido end) / count(distinct case when descSituacao = 'delivered' then idPedido end) as pctPedidoAtraso,
  count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) as pctPedidoCancelado,
  avg(totalFrete) as avgFrete,
  percentile(totalFrete, 0.5) as medianFrete,
  max(totalFrete) as maxFrete,
  min(totalFrete) as minFrete,
  avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) as qtdeDiasAprovadoEntrega,
  avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtPedido)) as qtdeDiasPedidoEntrega,
  avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) as qtdeDiasEntregaPromessa

from tb_pedido
group by idVendedor
