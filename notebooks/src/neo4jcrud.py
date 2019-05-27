from neo4j import GraphDatabase


class UrbsNeo4JDatabase(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def create_bus_company(self, company_code, company_name):
        with self._driver.session() as session:
            return session.run("CREATE (c:Company) "
                               " SET c.company_code = $company_code , c.company_name=$company_name RETURN id(c)",
                               company_code=company_code, company_name=company_name).single().value()

    def create_bus_category(self, category_code, category_name):
        with self._driver.session() as session:
            return session.run("CREATE (bc:BusCategory) "
                               " SET bc.category_code = $category_code , bc.category_name=$category_name RETURN id(bc)",
                               category_code=category_code, category_name=category_name).single().value()

    def create_bus_stop(self, name, number, type, latitude, longitude):
        with self._driver.session() as session:
            return session.run("CREATE (bs:BusStop) "
                               " SET bs.name = $name , bs.number=$number , bs.type=$type, "
                               "bs.latitude=$latitude, bs.longitude=$longitude return id(bs)",
                               name=name, number=number, type=type, latitude=latitude,
                               longitude=longitude).single().value()

    def create_bus_lines(self, start_point, end_point, line_code, line_way, service_category, line_name, color_name,
                         card_only):
        cipher_query = 'MATCH(bss: BusStop {number: $start_point}), (bse: BusStop {number: $end_point}) ' \
                       'CREATE(bss) - [: NEXT_STOP {' \
                       '  line_code: $line_code' \
                       ' ,line_way: $line_way' \
                       ' ,service_category: $service_category' \
                       ' ,line_name: $line_name' \
                       ' ,color_name: $color_name' \
                       ' ,card_only: $card_only' \
                       '}]->(bse)'

        with self._driver.session() as session:
            return session.run(cipher_query,
                               start_point=start_point
                               , end_point=end_point
                               , line_code=line_code
                               , line_way=line_way
                               , service_category=service_category
                               , line_name=line_name
                               , color_name=color_name
                               , card_only=card_only)

    def create_bus(self, vehicle):
        with self._driver.session() as session:
            return session.run("CREATE (b:Bus) "
                               " SET b.vehicle = $vehicle RETURN id(b)",
                               vehicle=vehicle).single().value()

    def create_position(self, vehicle, latitude, longitude,line_code, event_timestamp,date):
        with self._driver.session() as session:
            return session.run('CREATE (p:Position) '
                               ' SET p.vehicle = $vehicle '
                               ', p.coordinates = point({ latitude:toFloat($latitude),longitude:toFloat($longitude) }) '
                               ', p.event_timestamp = $event_timestamp '
                               ', p.date = $date '
                               ', p.line_code = $line_code '
                               ' RETURN id(p)',
                               vehicle=vehicle, latitude=latitude, longitude=longitude,
                               event_timestamp=str(event_timestamp),date=date,line_code=line_code).single().value()
    
    
    def connect_events(self,vehicle,line_code,date):
        
        connect_evt = "MATCH (p:Position {vehicle: $vehicle,line_code: $line_code,date: $date }) " \
                       "WITH p ORDER BY p.event_timestamp DESC " \
                       "WITH collect(p) as entries "  \
                       "FOREACH(i in RANGE(0, size(entries)-2) | " \
                       "FOREACH(e1 in [entries[i]] | " \
                       "FOREACH(e2 in [entries[i+1]] | " \
                       "MERGE (e2)-[ :MOVES_TO ]->(e1)))) "

        with self._driver.session() as session:
            return session.run(connect_evt, vehicle=vehicle,line_code=line_code,date=date)
        
    def create_edge_properties(self,vehicle,line_code,date):
        
        
        create_edge_properties = "MATCH (ps:Position {line_code:$line_code,vehicle:$vehicle,date: $date})-[m:MOVES_TO]->(pf:Position {line_code:$line_code,vehicle:$vehicle,date: $date}) " \
                                 "with ps.line_code as line_code,ps.vehicle as vehicle, ps.event_timestamp as start_date,pf.event_timestamp as end_date,distance(ps.coordinates,pf.coordinates) as delta_distance " \
                                 ",(datetime(pf.event_timestamp).epochMillis - datetime(ps.event_timestamp).epochMillis)/1000 as delta_time " \
                                 "MATCH (ps:Position {line_code:line_code,vehicle:vehicle,event_timestamp:start_date})-[m:MOVES_TO]->(pf:Position {line_code:line_code,vehicle:vehicle,event_timestamp:end_date}) " \
                                 "SET m.delta_distance=delta_distance , m.delta_time=delta_time"
        
        with self._driver.session() as session:
            return session.run(create_edge_properties, vehicle=vehicle,line_code=line_code,date=date)
        
        
    def delete_all(self):
        with self._driver.session() as session:
            return session.run("MATCH (n) DETACH DELETE n").single()
