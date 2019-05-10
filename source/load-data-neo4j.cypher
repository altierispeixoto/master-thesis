CALL spatial.addWKTLayer('layer_curitiba_neighbourhoods','geometry')

-- LOAD NEIGHBOURHOODS
LOAD CSV WITH HEADERS FROM "file:///bairros.csv" AS row  FIELDTERMINATOR ';'
CREATE (n:Neighbourhood)
    set        n.type         = row.TIPO
              ,n.name         = row.NOME
              ,n.geometry     = row.WKT
              ,n.section_code = row.CD_REGIONA
              ,n.section_name = row.NM_REGIONA
WITH n
CALL spatial.addNode('layer_curitiba_neighbourhoods',n) YIELD node
RETURN node;

-- LOAD HEALTH STATIONS
LOAD CSV WITH HEADERS FROM "file:///unidades-saude.csv" AS row
CREATE (p:Poi)
    set        p.category      = row.categoria
              ,p.name          = row.nome
              ,p.address       = row.endereco
              ,p.geometry      = 'POINT('+row.longitude +' '+row.latitude+')' 
              ,p.height        = row.elevacao
              ,p.neighbourhood = row.bairro
              ,p.district      = row.distrito
              ,p.source        = 'planilha'
WITH p
CALL spatial.addNode('layer_curitiba_neighbourhoods',p) YIELD node
RETURN node;

-- LOAD BUS STOPS
LOAD CSV WITH HEADERS FROM "file:///bus-stop.csv" AS row
create (bs:BusStop) 
set bs.name     = row.nome,
    bs.number   = row.num,
    bs.type     = row.tipo,
    bs.geometry = 'POINT(' + row.lon +' '+ row.lat +')'  
WITH bs
CALL spatial.addNode('layer_curitiba_neighbourhoods',bs) YIELD node
RETURN node;
