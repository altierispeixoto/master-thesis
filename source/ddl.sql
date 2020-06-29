set hive.exec.dynamic.partition=true

set hive.exec.dynamic.partition.mode=nonstrict

create external table linhas(
    categoria_servico string,
    cod               string,
    nome              string,
    nome_cor          string,
    somente_cartao    string,
    datareferencia    date
  ) PARTITIONED BY (year string, month string, day string)
    STORED AS PARQUET LOCATION '/usr/urbs/linhas';
msck repair table linhas;

select * from linhas
where year=2020 and month=2 limit 10;


create external table pontoslinha(
    cod            string,
    grupo          string,
    itinerary_id   string,
    lat            string,
    lon            string,
    nome           string,
    num            string,
    sentido        string,
    seq            string,
    tipo           string,
    datareferencia date
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET LOCATION '/usr/urbs/pontoslinha';
msck repair table pontoslinha;

select * from pontoslinha
where year=2020 and month=2 limit 10;

create external table tabelalinha(
    adapt          string,
    cod            string,
    dia            string,
    hora           string,
    num            string,
    ponto          string,
    tabela         string,
    datareferencia date
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET LOCATION '/usr/urbs/tabelalinha';
msck repair table tabelalinha;

select * from tabelalinha
where year=2020 and month=2 limit 10;


create external table tabelaveiculo(
    cod_linha      string,
    cod_ponto      string,
    horario        string,
    nome_linha     string,
    tabela         string,
    veiculo        string,
    datareferencia date
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET LOCATION '/usr/urbs/tabelaveiculo';
msck repair table tabelaveiculo;

select * from tabelaveiculo
where year=2020 and month=2 limit 10;


create external table trechositinerarios(
    codigo_urbs             string,
    cod_categoria           string,
    cod_empresa             string,
    cod_itinerario          string,
    cod_linha               string,
    cod_pto_parada_th       string,
    cod_pto_trecho_a        string,
    cod_pto_trecho_b        string,
    extensao_trecho_a_ate_b string,
    nome_categoria     string,
    nome_empresa       string,
    nome_itinerario    string,
    nome_linha         string,
    nome_pto_abreviado string,
    nome_pto_parada_th string,
    pto_especial       string,
    seq_ponto_trecho_a string,
    seq_ponto_trecho_b string,
    seq_pto_iti_th     string,
    stop_code       string,
    stop_name       string,
    tipo_trecho     string,
    datareferencia  date
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET LOCATION '/usr/urbs/trechositinerarios';
msck repair table trechositinerarios;

select * from trechositinerarios
where year=2020 and month=2 limit 10;


create external table veiculos(
   cod_linha       string,
   veic            string,
   lat             string,
   lon             string,
   event_timestamp string,
   hour            integer,
   minute         integer,
   second         integer,
   last_timestamp string,
   last_latitude  string,
   last_longitude string,
   delta_time     double,
   delta_distance double,
   delta_velocity double,
   moving_status  string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET LOCATION '/usr/urbs/veiculos';
msck repair table veiculos;

select * from veiculos
where year=2019 and month=2 limit 10;