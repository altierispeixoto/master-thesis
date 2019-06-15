CALL spatial.addWKTLayer('layer_curitiba','geometry');

// LOAD BUS LINES
LOAD CSV WITH HEADERS FROM "file:///line.csv" AS row
CREATE (l:Line)
    set   l.line_code  = row.cod
         ,l.category   = row.categoria
         ,l.name       = row.nome
         ,l.color      = row.color
         ,l.card_only  = row.somente_cartao
RETURN l;


// LOAD BUS STOPS
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///bus-stop.csv" AS row
create (bs:BusStop)
set bs.name       = row.nome
    ,bs.number    = row.num
    ,bs.type      = row.tipo
    ,bs.geometry  = 'POINT(' + row.lon +' '+ row.lat +')'
    ,bs.latitude  = row.lat
    ,bs.longitude = row.lon
WITH bs
CALL spatial.addNode('layer_curitiba',bs) YIELD node
RETURN node;

create index on :BusStop(number)

USING PERIODIC COMMIT 1000
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


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///trip-endpoints.csv" AS row
MATCH(l:Line {line_code:row.line_code}),(bs0:BusStop {number:row.origin}),(bs1:BusStop {number:row.destination})
MERGE(l)-[:HAS_TRIP]->(t:Trip {line_way:row.sentido})
MERGE(t)-[:STARTS_ON_POINT]->(bs0)
MERGE(t)-[:ENDS_ON_POINT]->(bs1)


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///trips.csv" AS row
MATCH (l:Line {line_code:row.line_code})-[:HAS_TRIP]->
      (t:Trip)-[:STARTS_ON_POINT]->(bss:BusStop {number:row.start_point}),(t)-[:ENDS_ON_POINT]->(bse:BusStop {number:row.end_point})
MERGE(t)-[:HAS_SCHEDULE_AT]->(s:Schedule {start_time:row.start_time,end_time:row.end_time,time_table:row.time_table,vehicle:row.vehicle})

//
// MERGE (year:Year {value: toInteger(row.year)})-[:CONTAINS]->
//       (month:Month {value: toInteger(row.month)})-[:CONTAINS]->
//       (day:Day {value: toInteger(row.day)})-[:CONTAINS]->
//       (hour:Hour {value: toInteger(row.hour)})-[:CONTAINS]->
//       (minute:Minute {value: toInteger(row.minute)})-[:CONTAINS]->
//       (second:Second {value: toInteger(row.second)})
//

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///stop-events.csv" AS row
with distinct row.vehicle  as vehicle
CREATE (v:Vehicle {vehicle:vehicle}) return count(v)

create index on :Vehicle(vehicle)

USING PERIODIC COMMIT 100000
LOAD CSV WITH HEADERS FROM "file:///stop-events.csv" AS row
CREATE (s:Stop {geometry : 'POINT(' + row.longitude +' '+ row.latitude +')', latitude:row.latitude, longitude:row.longitude,event_timestamp:row.stop_timestamp, event_time:row.event_time,line_code: row.cod_linha, vehicle:row.vehicle })
return count(s)


CREATE INDEX ON :Stop(event_timestamp,vehicle)
CREATE INDEX ON :Stop(line_code,latitude,longitude,vehicle,event_timestamp)


USING PERIODIC COMMIT 100000
LOAD CSV WITH HEADERS FROM "file:///tracking.csv" AS row
with row where toFloat(row.delta_time) <= 14400
MATCH (s0:Stop {event_timestamp:row.last_stop,vehicle:row.veic}),(s1:Stop {event_timestamp:row.current_stop,vehicle:row.veic})
MERGE (s0)-[m:MOVED_TO {delta_time: row.delta_time, delta_distance: row.delta_distance,delta_velocity:row.delta_velocity}]->(s1)
return s0,m,s1

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///event-stop-edges.csv" AS row
MATCH (s:Stop {line_code:row.line_code,latitude:row.latitude,longitude:row.longitude,vehicle:row.vehicle,event_time:row.event_time}),(bs:BusStop {number:row.bus_stop_number})
MERGE(s)-[:EVENT_STOP {line_way:row.line_way}]->(bs)
