-- LOAD HEALPH STATIONS
LOAD CSV WITH HEADERS FROM "file:///unidades-saude.csv" AS row
CREATE (p:Poi)
    set        p.category=      row.categoria
              ,p.name=          row.nome
              ,p.geometry=     'POINT('+row.longitude +' '+row.latitude+')' 
              ,p.height=        row.elevacao
              ,p.neighbourhood= row.bairro
              ,p.distrito=      row.distrito
              ,p.source=        'planilha'
WITH p
CALL spatial.addNode('cwb_neighbourhoods',p) YIELD node
RETURN node;
