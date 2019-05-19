MATCH (n)
DETACH DELETE n

MATCH ()-[r:IS_LOCATED]-()
DELETE r


CALL apoc.periodic.iterate(
"MATCH ()-[r:MOVES_TO]-() return r",
"with r  DELETE r", {batchSize:1000,iterateList:true, parallel:false})


CALL spatial.addWKTLayer('layer_curitiba_neighbourhoods','geometry')

USING PERIODIC COMMIT 1000000
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


USING PERIODIC COMMIT 10000
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


-- create a relationship between health stations and neighbourhood
MATCH (p:Poi )
WITH collect(p) as pois
   UNWIND pois as poi
       call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(poi.longitude),lat:toFloat(poi.latitude)},0.0001) yield node,distance
       with poi,  node  as n where 'Neighbourhood' IN LABELS(n)
       merge (poi)-[r:IS_LOCATED]->(n)  return poi,r,n


-- create a relationship between bust_stop and neighbourhood
call apoc.periodic.commit("
MATCH (bus_stop:BusStop ) WHERE NOT (bus_stop)-[:IS_LOCATED]->() with bus_stop limit {limit}
call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(bus_stop.longitude),lat:toFloat(bus_stop.latitude)},0.0001) yield node,distance
with bus_stop, node as n where 'Neighbourhood' IN LABELS(n)  merge (bus_stop)-[r:IS_LOCATED]->(n)  return count(bus_stop)",{limit:500})



-- caution : this can enter in an infinite loop
CALL apoc.periodic.commit(
  "match (p:Position)
   with distinct p.vehicle as vehicle,p.line_code as line_code,p.date as date limit 100
   MATCH (p0:Position {vehicle: vehicle, line_code: line_code, date: date })
    WITH p0 ORDER BY p0.event_timestamp DESC
       WITH collect(p0) as entries
         FOREACH(i in RANGE(0, size(entries)-2) |
           FOREACH(e1 in [entries[i]] |
             FOREACH(e2 in [entries[i+1]] |
               MERGE (e2)-[ :MOVES_TO ]->(e1)))) ", {limit:100})



match (p:Position)
with distinct p.vehicle as vehicle,p.line_code as line_code,p.date as date limit 10


MATCH (p:Position {vehicle: vehicle, line_code: line_code, date: date })
 WITH p ORDER BY p.event_timestamp DESC
    WITH collect(p) as entries
      FOREACH(i in RANGE(0, size(entries)-2) |
        FOREACH(e1 in [entries[i]] |
          FOREACH(e2 in [entries[i+1]] |
            MERGE (e2)-[ :MOVES_TO ]->(e1))))


CALL apoc.periodic.iterate(
"MATCH (ps:Position)-[m:MOVES_TO]->(pf:Position) where not exists(m.delta_time)
RETURN ps,pf,distance(ps.coordinates,pf.coordinates) as delta_distance,
 (datetime(pf.event_timestamp).epochMillis - datetime(ps.event_timestamp).epochMillis)/1000 as delta_time",
"MATCH (ps)-[m:MOVES_TO]->(pf) SET m.delta_distance = delta_distance, m.delta_time = delta_time", {batchSize:1000000,iterateList:true, parallel:false})


// call apoc.periodic.commit("
// match (p:Position) WHERE NOT ()-[:RTREE_REFERENCE]->(p:Position)
// WITH p limit {limit}
// CALL spatial.addNode('layer_curitiba_neighbourhoods',p) YIELD node
// RETURN count(p);
// ",{limit:10000})

-- old

 // match (p0:Position) with distinct p0.vehicle as vehicle,p0.line_code as line_code,p0.date as date
 //   MATCH (p:Position {vehicle: vehicle,line_code: line_code,date: date })
 //   WITH p ORDER BY p.event_timestamp DESC
 //       WITH collect(p) as entries
 //         FOREACH(i in RANGE(0, size(entries)-2) |
 //           FOREACH(e1 in [entries[i]] |
 //             FOREACH(e2 in [entries[i+1]] |
 //               MERGE (e2)-[ :MOVES_TO ]->(e1))));
 //
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
