
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
