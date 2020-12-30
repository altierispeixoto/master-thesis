import streamlit as st
from neo4j import GraphDatabase
import pandas as pd
import pydeck as pdk
import altair as alt

st.sidebar.title('Transporte público de Curitiba')

NEO4J_URI = 'bolt://localhost:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'h4ck3r'

_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def generate_bar_graph(df, n_pontos):
    c = alt.Chart(df.head(n_pontos), height=300, width=1000) \
        .mark_bar().encode(x='number', y='degree', color='number', tooltip=['name'])
    st.altair_chart(c)


def generate_scatterplot(df, n_pontos, metric):
    initial_view_state = pdk.ViewState(latitude=-25.476439269764, longitude=-49.28137052173, zoom=13, pitch=0)

    color_lookup = pdk.data_utils.assign_random_colors(df['degree'])
    df['color'] = df.apply(lambda row: color_lookup.get(row['degree']), axis=1)

    layers = [
        pdk.Layer(
            'ScatterplotLayer',
            data=df.head(n_pontos),
            pickable=True,
            get_position='[longitude, latitude]',
            get_color='[200, 30, 0, 160]',
            stroked=True, filled=True,
            get_radius="degree",
            elevation_scale=50,
        )
    ]

    tooltip = "numero:{number}\nnome:{name}\n" + metric + ":{degree}"

    r = pdk.Deck(map_style='mapbox://styles/mapbox/light-v9', initial_view_state=initial_view_state, layers=layers,
                 tooltip={"text": tooltip})

    st.pydeck_chart(r)


def bus_stops_centrality_degree(tx):
    bus_stops = []
    result = tx.run(
        """
         CALL algo.degree.stream("MATCH (n:BusStop) RETURN id(n) AS id","MATCH (n:BusStop)-[:NEXT_STOP]-(m:BusStop) 
         RETURN id(n) AS source, id(m) AS target",{graph: "cypher"}) 
         YIELD nodeId, score
         with algo.asNode(nodeId) as bus_stop, score 
         return bus_stop.number as bs_number, bus_stop.name as name, bus_stop.latitude as latitude , bus_stop.longitude as longitude, score 
         ORDER BY score DESC
        """
    )
    for record in result:
        bus_stops.append({'number': record['bs_number'], 'name': record['name'], 'latitude': float(record['latitude']),
                          'longitude': float(record['longitude']), 'degree': float(record['score'])})
    return bus_stops


def bus_stops_pagerank(tx):
    bus_stops = []
    result = tx.run(
        """
        CALL algo.pageRank.stream('BusStop', 'NEXT_STOP', {iterations:20, dampingFactor:0.85})
        YIELD nodeId, score
        with algo.asNode(nodeId) as bus_stop, score 
        return bus_stop.number as bs_number, bus_stop.name as name, bus_stop.latitude as latitude , bus_stop.longitude as longitude, score 
        ORDER BY score DESC
        """
    )
    for record in result:
        bus_stops.append({'number': record['bs_number'], 'name': record['name'], 'latitude': float(record['latitude']),
                          'longitude': float(record['longitude']), 'degree': float(record['score'])})
    return bus_stops


def bus_stops_betweeness(tx):
    bus_stops = []
    result = tx.run(
        """
        CALL algo.betweenness.stream('BusStop', 'NEXT_STOP', {direction:'out'})
        YIELD nodeId, centrality
        with algo.asNode(nodeId) as bus_stop, centrality as score 
        return bus_stop.number as bs_number, bus_stop.name as name, bus_stop.latitude as latitude , bus_stop.longitude as longitude, score 
        ORDER BY score DESC
        """
    )
    for record in result:
        bus_stops.append({'number': record['bs_number'], 'name': record['name'], 'latitude': float(record['latitude']),
                          'longitude': float(record['longitude']), 'degree': float(record['score'])})
    return bus_stops



def get_time_select(tx):
    rows = []
    result = tx.run(
        """
        MATCH (y:Year)-[:CONTAINS]->(m:Month)-[:CONTAINS]->(d:Day)-[:CONTAINS]-(h:Hour)-[:HAS_EVENT]->(n:Event) RETURN min(n.event_timestamp) as min_event_timestamp,max(n.event_timestamp) as max_event_timestamp
        """
    )
    for record in result:
        rows.append({'min_event_timestamp': record['min_event_timestamp'], 'max_event_timestamp': record['max_event_timestamp']})
    return rows

def get_events(tx):
    rows = []
    result = tx.run(
        """
        MATCH (y:Year)-[:CONTAINS]->(m:Month)-[:CONTAINS]->(d:Day)-[:HAS_LINE]->(l:Line)-[:HAS_TRIP]->(t:Trip)-[:HAS_BUS_STOP]->(bss:BusStop) 
        with y,m,d,l,t,bss where y.value in [2019] and m.value in [5] and d.value in [1] 
        MATCH (t)-[:HAS_EVENT]->(ev:Event)<-[:HAS_EVENT]-(h:Hour)<-[:CONTAINS]-(d) where h.value in [7,8]
        MATCH (ev)-[:IS_NEAR_BY]->(bss)  
        return  y.value as year, m.value as month, d.value as day,h.value as hour, bss.number as busstop_number, 
        bss.name as name, bss.latitude as latitude, bss.longitude as longitude, l.line_code as line_code,
        t.line_way as line_way, ev.vehicle as vehicle, ev.event_timestamp as event_timestamp
        """
    )
    for record in result:
        rows.append({'year': record['year'],
                     'month': record['month'],
                     'day': record['day'],
                     'hour': record['hour'],
                     'busstop_number': record['busstop_number'],
                     'name': record['name'],
                     'latitude':record['latitude'],
                     'longitude': record['longitude'],
                     'line_code': record['line_code'],
                     'line_way': record['line_way'],
                     'vehicle': record['vehicle'],
                     'event_timestamp':record['event_timestamp']})
    return rows



b = st.sidebar.selectbox('Metrica de Rede:',
                         ["Estática", "Dinâmica"])

if b == 'Estática':
    a = st.sidebar.selectbox('Metrica Rede Estática:',
                             ["Selecione", "Degree Centrality", "Betweeness Centrality", "PageRank"])

    if a == "Degree Centrality":

        with _driver.session() as session:
            degree_centrality = session.read_transaction(bus_stops_centrality_degree)
            df = pd.DataFrame(degree_centrality)
        _driver.close()

        st.subheader(a)

        n_pontos = st.sidebar.slider("Nº de pontos:", 5, 1000)
        tabela = st.sidebar.checkbox("Mostrar Tabela")

        if tabela:
            st.subheader('Tabela de Dados')
            st.table(df.head(n_pontos))
        else:
            generate_bar_graph(df, n_pontos)
            generate_scatterplot(df, n_pontos, a)

    elif a == 'PageRank':

        with _driver.session() as session:
            bus_stops_pagerank = session.read_transaction(bus_stops_pagerank)
            df = pd.DataFrame(bus_stops_pagerank)
        _driver.close()

        st.subheader(a)

        n_pontos = st.sidebar.slider("Nº de pontos:", 5, 1000)
        tabela = st.sidebar.checkbox("Mostrar Tabela")

        if tabela:
            st.subheader('Tabela de Dados')
            st.table(df.head(n_pontos))
        else:
            generate_bar_graph(df, n_pontos)
            generate_scatterplot(df, n_pontos, a)

    elif a == 'Betweeness Centrality':

        with _driver.session() as session:
            bus_stops_betweeness = session.read_transaction(bus_stops_betweeness)
            df = pd.DataFrame(bus_stops_betweeness)
        _driver.close()

        st.subheader(a)

        n_pontos = st.sidebar.slider("Nº de pontos:", 5, 1000)
        tabela = st.sidebar.checkbox("Mostrar Tabela")

        if tabela:
            st.subheader('Tabela de Dados')
            st.table(df.head(n_pontos))
        else:
            generate_bar_graph(df, n_pontos)
            generate_scatterplot(df, n_pontos, a)

            st.text(
                "Betweenness centrality is a way of detecting the amount of influence a node has over the flow of information in a graph. "
                "\n It is often used to find nodes that serve as a bridge from one part of a graph to another.")

            st.text("https://neo4j.com/docs/graph-algorithms/current/labs-algorithms/betweenness-centrality/")
else:
    import datetime

    with _driver.session() as session:
        time_tree = session.read_transaction(get_time_select)
        df_time_tree = pd.DataFrame(time_tree)
        df_time_tree['min_event_timestamp'] =  pd.to_datetime(df_time_tree['min_event_timestamp'],infer_datetime_format=True)
        df_time_tree['max_event_timestamp'] = pd.to_datetime(df_time_tree['max_event_timestamp'], infer_datetime_format=True)
    _driver.close()

    date = st.sidebar.date_input('Date', (df_time_tree['min_event_timestamp'][0], df_time_tree['max_event_timestamp'][0] ))
    st.write(date[0].year)

    date2 = st.sidebar.slider('Time range', min_value=1, max_value=23, value=(1, 23))
    st.write(date2[0])

    with _driver.session() as session:
        events = session.read_transaction(get_events)
        df_events = pd.DataFrame(events)
    _driver.close()

    st.table(df_events.head())

    #year_selected = st.sidebar.slider("Event Timestamp", min_value=min(df['min_event_timestamp']), max_value=max(df['max_event_timestamp']))

    # month_selected = st.sidebar.multiselect("Month", df['month'].unique())
    #
    # day_selected = st.sidebar.multiselect("Day", df['day'].unique())
    #
    # hour_selected = st.sidebar.slider("hour", df['hour'].unique())