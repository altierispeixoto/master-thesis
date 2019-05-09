-- LOAD HEALPH STATIONS
LOAD CSV WITH HEADERS FROM "file:///unidades-saude.csv" AS row
CREATE (p:Poi { category: row.categoria , name: row.nome, geometry : 'POINT('+row.longitude +' '+row.latitude+')' ,elevacao:row.elevacao, bairro:row.bairro,distrito:row.distrito,source:'planilha'})
WITH p
CALL spatial.addNode('cwb_neighbourhoods',p) YIELD node
RETURN node;
