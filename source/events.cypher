
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



// call apoc.periodic.commit("
// match (p:Position) WHERE NOT ()-[:RTREE_REFERENCE]->(p:Position)
// WITH p limit {limit}
// CALL spatial.addNode('layer_curitiba_neighbourhoods',p) YIELD node
// RETURN count(p);
// ",{limit:10000})
