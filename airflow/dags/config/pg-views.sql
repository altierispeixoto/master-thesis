create or replace view rota_sequenciada as
 select 	pseq.cod_linha
           ,pseq.sentido_linha
           ,pseq.seq_inicio
           ,pseq.seq_fim
           ,pseq.ponto_inicio
           ,pseq.nome_ponto_inicio
           ,pseq.ponto_final
           ,pseq.nome_ponto_final
           ,li.CATEGORIA_SERVICO as categoria_servico
           ,li.NOME as nome_linha
           ,li.NOME_COR as nome_cor
           ,li.SOMENTE_CARTAO as somente_cartao
           ,pseq.datareferencia
                 from (select
                               p1.COD as cod_linha
                              ,p1.SENTIDO  as sentido_linha
                              ,p1.SEQ      as seq_inicio
                              ,p2.SEQ      as seq_fim
                              ,p1.NUM      as ponto_inicio
                              ,p1.NOME     as nome_ponto_inicio
                              ,p2.NUM      as ponto_final
                              ,p2.NOME     as nome_ponto_final
                              ,p1.datareferencia
                              from pontoslinha_stg P1
                              inner join pontoslinha_stg p2
                                  on (cast(p1.SEQ as int) +1 = cast(p2.SEQ as int)
                                       and p1.COD = p2.COD
                                       and p1.SENTIDO = p2.SENTIDO
                                       and p1.datareferencia = p2.datareferencia)
                              ) pseq
                              inner join linhas_stg li
                                     on (pseq.cod_linha = li.COD and pseq.datareferencia = li.datareferencia)
                              order by pseq.cod_linha
                                      ,pseq.sentido_linha
                                      ,pseq.seq_inicio
                                      ,pseq.seq_fim;