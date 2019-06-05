CALL spatial.addWKTLayer('layer_curitiba','geometry')


-- LOAD SECTIONS
LOAD CSV WITH HEADERS FROM "file:///section.csv" AS row
CREATE (s:Section)
    set   s.geometry     = row.WKT
         ,s.section_name = row.NOME
WITH s
CALL spatial.addNode('layer_curitiba',s) YIELD node
RETURN node;


-- LOAD NEIGHBOURHOODS
LOAD CSV WITH HEADERS FROM "file:///neighbourhood.csv" AS row
CREATE (n:Neighbourhood)
    set
         n.name         = row.NOME
        ,n.geometry     = row.WKT
        ,n.section_name = row.NM_REGIONAL
WITH n
CALL spatial.addNode('layer_curitiba',n) YIELD node
RETURN node;


-- LOAD HEALTH STATIONS
LOAD CSV WITH HEADERS FROM "file:///unidades-saude.csv" AS row
CREATE (p:Poi:HealthStation)
    set        p.category      = row.categoria
              ,p.name          = row.nome
              ,p.address       = row.endereco
              ,p.geometry      = 'POINT('+row.longitude +' '+row.latitude+')'
              ,p.neighbourhood = row.bairro
              ,p.section_name  = row.distrito
              ,p.latitude      = row.latitude
              ,p.longitude     = row.longitude
WITH p
CALL spatial.addNode('layer_curitiba',p) YIELD node
RETURN node;


-- create a relationship between health stations and neighbourhoods
MATCH (p:Poi)
WITH collect(p) as pois
   UNWIND pois as poi
       call spatial.withinDistance('layer_curitiba',{lon:toFloat(poi.longitude),lat:toFloat(poi.latitude)},0.0001) yield node,distance
       with poi,  node  as n where 'Neighbourhood' IN LABELS(n)
       merge (poi)-[r:IS_IN_NEIGHBOURHOOD]->(n)  return poi,r,n

match (p:Poi)-[:IS_IN_NEIGHBOURHOOD]->(n:Neighbourhood) set p.neighbourhood = n.name


-- create a relationship between health stations and sections
MATCH (p:Poi)
WITH collect(p) as pois
   UNWIND pois as poi
       call spatial.withinDistance('layer_curitiba',{lon:toFloat(poi.longitude),lat:toFloat(poi.latitude)},0.0001) yield node,distance
       with poi,  node  as n where 'Section' IN LABELS(n)
       merge (poi)-[r:IS_IN_SECTION]->(n)  return poi,r,n

match (p:Poi)-[:IS_IN_SECTION]->(s:Section) set p.section_name = s.section_name


-- LOAD BUS TERMINAL STATIONS
LOAD CSV WITH HEADERS FROM "file:///bus-terminal-station.csv" AS row
CREATE (ts:TerminalStation)
    set   ts.geometry        = row.WKT
          ,ts.name           = row.NOME_MAPA
          ,ts.neighbourhood  = row.BAIRRO
          ,ts.section_name   = row.REGIONAL
          ,ts.latitude       = row.LATITUDE
          ,ts.longitude      = row.LONGITUDE
WITH ts
CALL spatial.addNode('layer_curitiba',ts) YIELD node
RETURN node;


-- create a relationship between bus terminal stations and neighbourhoods
MATCH (t:TerminalStation)
WITH collect(t) as terminalStations
   UNWIND terminalStations as ts
       call spatial.withinDistance('layer_curitiba',{lon:toFloat(ts.longitude),lat:toFloat(ts.latitude)},0.0001) yield node,distance
       with ts, node  as n where 'Neighbourhood' IN LABELS(n)
       merge (ts)-[r:IS_IN_NEIGHBOURHOOD]->(n)  return ts,r,n

match (ts:TerminalStation)-[:IS_IN_NEIGHBOURHOOD]->(n:Neighbourhood) set ts.neighbourhood = n.name


-- create a relationship between health stations and sections
MATCH (t:TerminalStation)
WITH collect(t) as terminalStations
   UNWIND terminalStations as ts
       call spatial.withinDistance('layer_curitiba',{lon:toFloat(ts.longitude),lat:toFloat(ts.latitude)},0.0001) yield node,distance
       with ts,  node  as n where 'Section' IN LABELS(n)
       merge (ts)-[r:IS_IN_SECTION]->(n)  return ts,r,n

match (ts:TerminalStation)-[:IS_IN_SECTION]->(s:Section) set ts.section_name = s.section_name





-- LOAD BUS LINES
LOAD CSV WITH HEADERS FROM "file:///line.csv" AS row
CREATE (l:Line)
    set   l.line_code  = row.cod
         ,l.category   = row.categoria
         ,l.name       = row.nome
         ,l.color      = row.color
         ,l.card_only  = row.somente_cartao
RETURN l;


-- LOAD BUS STOPS
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


//TODO: IMPROVE PERFORMANCE
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


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///trip-endpoints.csv" AS row
MATCH(l:Line {line_code:row.line_code}),(bs0:BusStop {number:row.origin}),(bs1:BusStop {number:row.destination})
MERGE(l)-[:HAS_TRIP]->(t:Trip {line_way:row.sentido})
MERGE(t)-[:STARTS_AT]->(bs0)
MERGE(t)-[:ENDS_AT]->(bs1)


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///schedules.csv" AS row
MATCH(bs:BusStop {number:row.cod_ponto})
MERGE (bs)-[:HAS_SCHEDULE_AT]-(s:Schedule {line_code:row.cod_linha,time:row.horario,line_name:row.nome_linha,time_table:row.tabela,vehicle:row.veiculo})



MATCH (p1:BusStop)-[r:NEXT_STOP]->(p2:BusStop)
SET r.distance = distance(point({longitude: toFloat(p1.longitude),latitude: toFloat(p1.latitude) ,crs: 'wgs-84'}),point({longitude: toFloat(p2.longitude),latitude: toFloat(p2.latitude) ,crs: 'wgs-84'}))



MATCH(ts:TerminalStation {name:"TERMINAL PORTÃO"}),(bs:BusStop) where bs.name contains "Terminal Port?o"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CAPÃO RASO"}),(bs:BusStop) where bs.name contains "Terminal Cap?o Raso"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL PINHEIRINHO"}),(bs:BusStop) where bs.name contains "Terminal Pinheirinho"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL HAUER"}),(bs:BusStop) where bs.name contains "Terminal Hauer"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CABRAL"}),(bs:BusStop) where bs.name contains "Terminal Cabral"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CAMPO COMPRIDO"}),(bs:BusStop) where bs.name contains "Terminal Campo Comprido"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL SANTA FELICIDADE"}),(bs:BusStop) where bs.name contains "Terminal Santa Felicidade"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL OFICINAS"}),(bs:BusStop) where bs.name contains "Terminal Oficinas"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CAMPINA DO SIQUEIRA"}),(bs:BusStop) where bs.name contains "Terminal Campina do Siqueira"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL SANTA CÂNDIDA"}),(bs:BusStop) where bs.name contains "Terminal Santa Candida"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CAIUÁ"}),(bs:BusStop) where bs.name contains "Terminal Caiua"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL BOQUEIRÃO"}),(bs:BusStop) where bs.name contains "Terminal Boqueir?o"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL FAZENDINHA"}),(bs:BusStop) where bs.name contains "Terminal Fazendinha"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL SÍTIO CERCADO"}),(bs:BusStop) where bs.name contains "Terminal Sitio Cercado"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL BAIRRO ALTO"}),(bs:BusStop) where bs.name contains "Terminal Bairro Alto"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CIC"}),(bs:BusStop) where bs.name contains "Terminal CIC"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CENTENÁRIO"}),(bs:BusStop) where bs.name contains "Terminal Centenario"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL BOA VISTA"}),(bs:BusStop) where bs.name contains "Terminal Boa Vista"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CARMO"}),(bs:BusStop) where bs.name contains "Terminal Carmo"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL CAPÃO DA IMBUIA"}),(bs:BusStop) where bs.name contains "Terminal Cap?o da Imbuia"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL GUADALUPE"}),(bs:BusStop) where bs.name contains "Terminal Guadalupe"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts


MATCH(ts:TerminalStation {name:"TERMINAL BARREIRINHA"}),(bs:BusStop) where bs.name contains "Terminal Barreirinha"
with bs,ts
merge (bs)-[r:IS_IN_TERMINAL]->(ts)
return bs,r,ts

CREATE INDEX ON :BusStop(id)
CREATE INDEX ON :IS_IN_SECTION(id)
CREATE INDEX ON :IS_IN_NEIGHBOURHOOD(id)


//TODO: IMPROVE PERFORMANCE
-- create a relationship between bust_stop and neighbourhood
call apoc.periodic.commit("
      MATCH (bs:BusStop ) WHERE NOT (bs)-[:IS_IN_NEIGHBOURHOOD]->() with bs limit {limit}
      CALL spatial.withinDistance('layer_curitiba',{lon:toFloat(bs.longitude),lat:toFloat(bs.latitude)},0.0001) yield node,distance
      WITH bs, node as n where 'Neighbourhood' IN LABELS(n)  merge (bs)-[r:IS_IN_NEIGHBOURHOOD]->(n)  return count(bs)",{limit:100})


match (bs:BusStop)-[:IS_IN_NEIGHBOURHOOD]->(n:Neighbourhood) set  bs.neighbourhood = n.name


call apoc.periodic.commit("
MATCH (bs:BusStop) WHERE NOT (bs)-[:IS_IN_SECTION]->() with bs limit {limit}
call spatial.withinDistance('layer_curitiba',{lon:toFloat(bs.longitude),lat:toFloat(bs.latitude)},0.0001) yield node,distance
with bs, node as n where 'Section' IN LABELS(n)  merge (bs)-[r:IS_IN_SECTION]->(n)  return count(bs)",{limit:500})


match (bs:BusStop)-[:IS_IN_SECTION]->(s:Section) set bs.section_name = s.section_name


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///stop-events.csv" AS row
MERGE (year:Year {value: toInteger(row.year)})-[:CONTAINS]->
      (month:Month {value: toInteger(row.month)})-[:CONTAINS]->
      (day:Day {value: toInteger(row.day)})-[:CONTAINS]->
      (hour:Hour {value: toInteger(row.hour)})-[:CONTAINS]->
      (minute:Minute {value: toInteger(row.minute)})-[:CONTAINS]->
      (second:Second {value: toInteger(row.second)})
MERGE (l:Line {line: row.cod_linha})-[:HAS_VEHICLE]->
      (v:Vehicle {vehicle:row.vehicle})-[:STOPPED_AT]->
      (s:Stop {geometry : 'POINT(' + row.longitude +' '+ row.latitude +')', latitude:row.latitude, longitude:row.longitude,timestamp:row.stop_timestamp })
with s,second
CALL spatial.addNode('layer_curitiba',s) YIELD node
MERGE (node)-[:HAPPENED_ON]-(second)



LOAD CSV WITH HEADERS FROM "file:///tracking.csv" AS row
MATCH (v:Vehicle {vehicle: row.veic})-[:STOPPED_AT]->(s0:Stop {timestamp:row.last_stop})
MATCH (v1:Vehicle {vehicle: row.veic})-[:STOPPED_AT]->(s1:Stop {timestamp:row.current_stop})
MERGE (s0)-[m:MOVED_TO {delta_time: row.delta_time, delta_distance: row.delta_distance,delta_velocity:row.delta_velocity}]->(s1)
return s0,m,s1



// cod_linha,veic,delta_time,delta_distance,delta_velocity,last_stop,current_stop
// 001,BN997,0.67,0.03,3.06,2019-02-21 06:45:31,2019-02-21 06:46:11


MATCH (s:Stop )
call spatial.withinDistance('layer_curitiba',{lon:toFloat(s.longitude),lat:toFloat(s.latitude)},0.01) yield node,distance
with s, node as n where 'BusStop' IN LABELS(n)  MERGE(s)-[:STOPS_AT]-(n)



-----------------------------------------------------------------------




MATCH (bus_stop:BusStop {section_name:'REGIONAL SANTA FELICIDADE / REGIONAL PORTAO'})
call spatial.withinDistance('layer_curitiba_neighbourhoods',{lon:toFloat(bus_stop.longitude),lat:toFloat(bus_stop.latitude)},0.0001) yield node,distance
with bus_stop, node as n where 'Section' IN LABELS(n)  match(bus_stop) set bus_stop.section_name = n.section_name


match(bs:BusStop {section_name:'REGIONAL BOQUEIR�O'}) set bs.section_name='REGIONAL BOQUEIRAO'
match (bs:BusStop {section_name:'PORT�O'})  set bs.section_name='REGIONAL PORTAO'
match (bs:BusStop {section_name:'SANTA FELICIDADE'})  set bs.section_name='REGIONAL SANTA FELICIDADE'
match (bs:BusStop {section_name:'CIC'})  set bs.section_name='REGIONAL CIC'

match(bs:BusStop {neighbourhood:'CAMPO COMPRIDO',section_puC'}) set bs.section_name='REGIONAL SANTA FELICIDADE'





MATCH (poi:Poi{category:'Unidade Saude Basica',source:'planilha'}),(bs:BusStop)
WHERE distance(point({longitude: toFloat(poi.longitude),latitude: toFloat(poi.latitude) ,crs: 'wgs-84'}) ,point({longitude: toFloat(bs.longitude),latitude: toFloat(bs.latitude) ,crs: 'wgs-84'})) < 2000
MERGE (bs)-[:WALK]->(poi)


MATCH (bs:BusStop)-[r:WALK]->(poi:Poi)
SET r.distance= distance(point({longitude: toFloat(poi.longitude),latitude: toFloat(poi.latitude) ,crs: 'wgs-84'})
,point({longitude: toFloat(bs.longitude),latitude: toFloat(bs.latitude) ,crs: 'wgs-84'}))
