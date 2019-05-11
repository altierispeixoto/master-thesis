MATCH (n)
DETACH DELETE n

MATCH ()-[r:IS_LOCATED]-() 
DELETE r

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
              ,p.latitude      = row.latitude
              ,p.longitude     = row.longitude
WITH p
CALL spatial.addNode('layer_curitiba_neighbourhoods',p) YIELD node
RETURN node;


-- LOAD BUS STOPS
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///bus-stop.csv" AS row
create (bs:BusStop) 
set bs.name       = row.nome
    ,bs.number    = row.num
    ,bs.type      = row.tipo
    ,bs.geometry  = 'POINT(' + row.lon +' '+ row.lat +')'  
    ,bs.latitude  = row.lat
    ,bs.longitude = row.lon
WITH bs
CALL spatial.addNode('layer_curitiba_neighbourhoods',bs) YIELD node
RETURN node;

-- create a relationship between health stations and neighbourhood
MATCH (p:Poi )
WITH collect(p) as pois
   UNWIND pois as poi 
       call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(poi.longitude),lat:toFloat(poi.latitude)},0.00001) yield node,distance 
       with poi,  node  as n where 'Neighbourhood' IN LABELS(n) 
       merge (poi)-[r:IS_LOCATED]->(n)  return poi,r,n 

-- create a relationship between bust_stop and neighbourhood
call apoc.periodic.commit("
MATCH (bus_stop:BusStop ) WHERE NOT (bus_stop)-[:IS_LOCATED]->() with bus_stop limit {limit}  
call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(bus_stop.longitude),lat:toFloat(bus_stop.latitude)},0.0001) yield node,distance 
with bus_stop, node as n where 'Neighbourhood' IN LABELS(n)  merge (bus_stop)-[r:IS_LOCATED]->(n)  return count(bus_stop)",{limit:500})


-- queries 

-- pois inside pinheirinho
match(p:Poi) where p.neighbourhood = 'Pinheirinho' 
call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(p.longitude),lat:toFloat(p.latitude)},0.00001) yield node,distance 
with p,  node  as n where 'Neighbourhood' IN LABELS(n)   
merge (p)-[r:IS_LOCATED]->(n)  return p,r,n

-- distance between point of interest and bus_stop
match (p:Poi)-[r:IS_LOCATED]->(n:Neighbourhood)<-[r0:IS_LOCATED]-(bs:BusStop) 
return n.name,n.section_name,p.name,p.latitude,p.longitude,bs.name,bs.latitude,bs.longitude,
distance(point({longitude: toFloat(p.longitude),latitude: toFloat(p.latitude) ,crs: 'wgs-84'}) ,point({longitude: toFloat(bs.longitude),latitude: toFloat(bs.latitude) ,crs: 'wgs-84'})) AS distance limit 100

