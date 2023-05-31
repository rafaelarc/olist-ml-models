-- Databricks notebook source
with tb_pedido as (

  select distinct
    t1.idPedido,
    t2.idVendedor

  from silver.olist.pedido as t1

  left join silver.olist.item_pedido as t2
  on t1.idPedido = t2.idPedido

  where t1.dtPedido < '2018-01-01'
  and t1.dtPedido >= add_months('2018-01-01', -6)
  and idVendedor is not null

),

tb_join as (

  select
    t1.idVendedor,
    t2.*
  
  from tb_pedido as t1

  left join silver.olist.pagamento_pedido as t2
  on t1.idPedido = t2.idPedido

),

tb_group as (
  
  select idVendedor,
        descTipoPagamento,
        count(distinct idPedido) as qtdePedidoMeioPagamento,
        sum(vlPagamento) as vlPedidoMeioPagamento

  from tb_join

  group by idVendedor, descTipoPagamento
  order by idVendedor, descTipoPagamento

),

tb_summary as (

  select 
    idVendedor,

    sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido

    from tb_group
    
    group by idVendedor

),

tb_cartao as (

  select idVendedor,
        avg(nrParcelas) as avgQtdeParcelas,
        percentile(nrParcelas, 0.5) as medianQtdeParcelas,
        max(nrParcelas) as maxQtdeParcelas,
        min(nrParcelas) as minQtdeParcelas

  from tb_join

  where descTipoPagamento = 'credit_card'

  group by idVendedor
)

select 
  '2018-01-01' as dtReference,
  date(now()) as dtIngestion,
  t1.*,
  t2.avgQtdeParcelas,
  t2.medianQtdeParcelas,
  t2.maxQtdeParcelas,
  t2.minQtdeParcelas

from tb_summary as t1

left join tb_cartao as t2
on t1.idVendedor = t2.idVendedor

