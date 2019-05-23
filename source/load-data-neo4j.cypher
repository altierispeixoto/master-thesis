MATCH (n) DETACH DELETE n

MATCH ()-[r:IS_LOCATED]-() DELETE r

CALL apoc.periodic.iterate("MATCH ()-[r:MOVES_TO]-() return r","with r  DELETE r", {batchSize:1000,iterateList:true, parallel:false})


--------

CALL spatial.addWKTLayer('layer_curitiba_neighbourhoods','geometry')


-- LOAD SECTIONS
LOAD CSV WITH HEADERS FROM "file:///distritos.csv" AS row  FIELDTERMINATOR ';'
CREATE (s:Section)
    set   s.geometry     = row.WKT
         ,s.section_name = row.DISTRITO
WITH s
CALL spatial.addNode('layer_curitiba_neighbourhoods',s) YIELD node
RETURN node;


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


-- create a relationship between health stations and sections
MATCH (p:Poi )
WITH collect(p) as pois
   UNWIND pois as poi
       call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(poi.longitude),lat:toFloat(poi.latitude)},0.0001) yield node,distance
       with poi,  node  as n where 'Section' IN LABELS(n)
       merge (poi)-[r:IS_LOCATED]->(n)  return poi,r,n


match (p:Poi)-[:IS_LOCATED]->(s:Section) set p.section_name = s.section_name


-- create a relationship between health stations and neighbourhoods
MATCH (p:Poi )
WITH collect(p) as pois
   UNWIND pois as poi
       call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(poi.longitude),lat:toFloat(poi.latitude)},0.0001) yield node,distance
       with poi,  node  as n where 'Neighbourhood' IN LABELS(n)
       merge (poi)-[r:IS_LOCATED]->(n)  return poi,r,n


match (p:Poi)-[:IS_LOCATED]->(n:Neighbourhood) set p.neighbourhood = n.name


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


USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///routes.csv" AS row
MATCH(bss: BusStop {number: row.ponto_inicio}), (bse: BusStop {number: row.ponto_final})
CREATE(bss) - [: NEXT_STOP {
    line_code: row.cod_linha
   ,line_way: row.sentido_linha
   ,service_category: row.categoria_servico
   ,line_name: row.nome_linha
   ,color_name: row.nome_cor
   ,card_only: row.somente_cartao
   }]->(bse)


MATCH (p1:BusStop)-[r:NEXT_STOP]->(p2:BusStop)
SET r.distance = distance(point({longitude: toFloat(p1.longitude),latitude: toFloat(p1.latitude) ,crs: 'wgs-84'})
,point({longitude: toFloat(p2.longitude),latitude: toFloat(p2.latitude) ,crs: 'wgs-84'}))


call apoc.periodic.commit("
MATCH (bus_stop:BusStop ) WHERE NOT (bus_stop)-[:IS_IN_SECTION]->() with bus_stop limit {limit}
call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(bus_stop.longitude),lat:toFloat(bus_stop.latitude)},0.0010) yield node,distance
with bus_stop, node as n where 'Section' IN LABELS(n)  merge (bus_stop)-[r:IS_IN_SECTION]->(n)  return count(bus_stop)",{limit:500})

match (bs:BusStop)-[:IS_IN_SECTION]->(s:Section) set bs.section_name = s.section_name


-- create a relationship between bust_stop and neighbourhood
call apoc.periodic.commit("
MATCH (bus_stop:BusStop ) WHERE NOT (bus_stop)-[:IS_LOCATED]->() with bus_stop limit {limit}
call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(bus_stop.longitude),lat:toFloat(bus_stop.latitude)},0.0001) yield node,distance
with bus_stop, node as n where 'Neighbourhood' IN LABELS(n)  merge (bus_stop)-[r:IS_LOCATED]->(n)  return count(bus_stop)",{limit:500})


match (bs:BusStop)-[:IS_LOCATED]->(n:Neighbourhood) set bs.neighbourhood = n.name



USING PERIODIC COMMIT 100000
LOAD CSV WITH HEADERS FROM "file:///events.csv" AS row
CREATE(p:Position)
SET p.vehicle = row.veic
,p.event_timestamp = datetime(row.event_timestamp)
,p.date = row.date
,p.line_code = row.cod_linha
,p.geometry  = 'POINT(' + row.lon +' '+ row.lat +')'
,p.latitude  = toFloat(row.lat)
,p.longitude = toFloat(row.lon);

create index on :Position(vehicle,line_code,date )


CALL apoc.periodic.iterate(
"MATCH (ps:Position)-[m:MOVES_TO]->(pf:Position) where not exists(m.delta_time)
RETURN ps,pf, distance(ps.coordinates,pf.coordinates) as delta_distance,
 (datetime(pf.event_timestamp).epochMillis - datetime(ps.event_timestamp).epochMillis)/1000 as delta_time",
"MATCH (ps)-[m:MOVES_TO]->(pf) SET m.delta_distance = delta_distance, m.delta_time = delta_time", {batchSize:1000000,iterateList:true, parallel:false})


MATCH (poi:Poi{category:'Unidade Saude Basica',source:'planilha'}),(bs:BusStop)
WHERE distance(point({longitude: toFloat(poi.longitude),latitude: toFloat(poi.latitude) ,crs: 'wgs-84'}) ,point({longitude: toFloat(bs.longitude),latitude: toFloat(bs.latitude) ,crs: 'wgs-84'})) < 500
CREATE (bs)-[:WALK]->(poi)



MATCH (bs:BusStop)-[r:WALK]->(poi:Poi)
SET r.distance= distance(point({longitude: toFloat(poi.longitude),latitude: toFloat(poi.latitude) ,crs: 'wgs-84'})
,point({longitude: toFloat(bs.longitude),latitude: toFloat(bs.latitude) ,crs: 'wgs-84'}))

// MATCH (p:BusStop)-[r:WALK]->(poi:Poi)
// SET r.distance=round((2 * 6371 * asin(sqrt(haversin(radians(toFloat(p.latitude) - toFloat(poi.latitude)))
// + cos(radians(toFloat(p.latitude)))* cos(radians(toFloat(poi.latitude)))* haversin(radians(toFloat(p.longitude) - toFloat(poi.longitude))))))*100)/100


// call apoc.periodic.commit("
// match (p:Position) WHERE NOT ()-[:RTREE_REFERENCE]->(p:Position)
// WITH p limit {limit}
// CALL spatial.addNode('layer_curitiba_neighbourhoods',p) YIELD node
// RETURN count(p);
// ",{limit:10000})


-- distance between point of interest and bus_stop
match (p:Poi)-[r:IS_LOCATED]->(n:Neighbourhood)<-[r0:IS_LOCATED]-(bs:BusStop)
return n.name,n.section_name,p.name,p.latitude,p.longitude,bs.name,bs.latitude,bs.longitude,
distance(point({longitude: toFloat(p.longitude),latitude: toFloat(p.latitude) ,crs: 'wgs-84'}) ,point({longitude: toFloat(bs.longitude),latitude: toFloat(bs.latitude) ,crs: 'wgs-84'})) AS distance limit 100



//calculates minimum distance from every bus stop to its closest health unity in the same district:
MATCH (bs:BusStop),(poi:Poi)
WHERE bs.section_name = poi.section_name
CALL apoc.algo.dijkstra(bs, poi, 'NEXT_STOP|WALK', 'distance') YIELD weight
RETURN bs.section_name as regional
      ,bs.neighbourhood as bairro
      ,bs.number AS numero
      ,bs.name as bus_stop
      ,bs.latitude as latitude
      ,bs.longitude as longitude
      ,poi.name as nome_us
      ,poi.section_name,poi.latitude as us_latitude, poi.longitude as us_longitude, min(weight) as distancia



MATCH (p:PontoLinha),(poi:Poi{categoria:'Unidade Saude Basica',source:'planilha'})
WHERE p.regional = poi.distrito
CALL apoc.algo.dijkstra(p, poi, 'proximo|caminhar', 'distancia') YIELD weight
RETURN p.numero AS numero, p.latitude as latitude, p.longitude as longitude, min(weight) as distancia