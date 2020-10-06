#./bin/bash
ROOT_PATH="/usr/local/airflow"

FILES=(color service_category lines trips bus_stop_type bus_stops line_routes trip_endpoints bus_event_edges events)

datareferencia={{params.datareferencia}}
echo $datareferencia

for file in "${FILES[@]}"; do
  mkdir -p $ROOT_PATH/neo4j/import/$file/$datareferencia
  cp $ROOT_PATH/data/neo4j/$file/*.csv $ROOT_PATH/neo4j/import/$file/$datareferencia/$file.csv
done
