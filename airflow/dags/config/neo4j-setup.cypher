CALL spatial.addWKTLayer('layer_curitiba','geometry');
create index on :BusStop (number)
create index on :BusStop (latitude,longitude)

create index on :Stop (line_code ,latitude ,longitude,vehicle,event_time)
create index on :Stop (event_timestamp ,vehicle)
