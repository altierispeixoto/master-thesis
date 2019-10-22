LOAD CSV WITH HEADERS FROM "file:///line.csv" AS row
CREATE (l:Line)
    set   l.line_code  = row.cod
         ,l.category   = row.categoria
         ,l.name       = row.nome
         ,l.color      = row.color
         ,l.card_only  = row.somente_cartao
RETURN l;