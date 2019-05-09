CALL spatial.addWKTLayer('layer_curitiba_neighbourhoods','geometry')
CALL spatial.importShapefileToLayer('layer_curitiba_neighbourhoods','import/bairros/DIVISA_DE_BAIRROS.shp')

-- LOAD HEALTH STATIONS
LOAD CSV WITH HEADERS FROM "file:///unidades-saude.csv" AS row
CREATE UNIQUE (p:Poi)
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
