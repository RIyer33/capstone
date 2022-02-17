CREATE TABLE IF NOT EXISTS public.staging_visa(
    visa_cat_code int4 ,
    visa_category varchar(256)
);

CREATE TABLE IF NOT EXISTS public.dim_visa(
    visa_cat_id int4 NOT NULL,
    visa_category varchar(256),
    CONSTRAINT pk_visa_id PRIMARY KEY (visa_cat_id)
);

CREATE TABLE IF NOT EXISTS public.staging_travel_mode(
    travel_mode_code int4 ,
    travel_mode varchar(25)
);

CREATE TABLE IF NOT EXISTS public.dim_travel_mode(
    travel_mode_id int4 NOT NULL,
    travel_mode varchar(25),
    CONSTRAINT pk_travel_mode_id PRIMARY KEY (travel_mode_id)
);

CREATE TABLE IF NOT EXISTS public.staging_country(
    country_code int4, 
    country_name varchar(256)
);

CREATE TABLE IF NOT EXISTS public.dim_country(
    country_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    country_code int4, 
    country_name varchar(256),
    CONSTRAINT pk_country_id PRIMARY KEY (country_id)
);

CREATE TABLE IF NOT EXISTS public.staging_ports(
    port_code varchar(3),
    port_name varchar(256)
);

CREATE TABLE IF NOT EXISTS public.dim_ports(
    port_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    port_code varchar(3),
    port_name varchar(256),
    CONSTRAINT pk_port_id PRIMARY KEY (port_id)
);

CREATE TABLE IF NOT EXISTS public.staging_date(
    date date ,
    date_id varchar(10),
    year int4,
    month int4,
    day int4,
    week int4,
    month_name varchar(20),
    day_of_week int4,
    day_of_week_name varchar(20)
);

CREATE TABLE IF NOT EXISTS public.dim_date(
    date_id int4 NOT NULL,
    date date NOT NULL,
    year int4,
    month int4,
    day int4,
    week int4,
    month_name varchar(20),
    day_of_week int4,
    day_of_week_name varchar(20),
    CONSTRAINT pk_date_id PRIMARY KEY (date_id)
);

CREATE TABLE IF NOT EXISTS public.staging_airports(
    airport_code varchar(10) ,
    airport_type varchar(25) ,
    airport_name varchar(50) ,
    elevation_ft DOUBLE PRECISION,
    continent varchar(10),
    iso_country varchar(5),
    region varchar(5),
    municipality varchar(20),
    gps_code varchar(10),
    iata_code varchar(10),
    local_code varchar(10),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS public.dim_airports(
    airport_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    airport_code varchar(10) NOT NULL,
    airport_type varchar(25) NOT NULL,
    airport_name varchar(50) NOT NULL,
    elevation_ft DOUBLE PRECISION,
    continent varchar(10),
    iso_country varchar(5),
    region varchar(5),
    municipality varchar(20),
    gps_code varchar(10),
    iata_code varchar(10),
    local_code varchar(10),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    CONSTRAINT pk_airport_id PRIMARY KEY (airport_id) 
);

CREATE TABLE IF NOT EXISTS public.staging_climate(
    city varchar(25) ,
    country varchar(25) ,
    avg_temp_jan DOUBLE PRECISION,
    avg_temp_feb DOUBLE PRECISION,
    avg_temp_mar DOUBLE PRECISION,
    avg_temp_apr DOUBLE PRECISION,
    avg_temp_may DOUBLE PRECISION,
    avg_temp_jun DOUBLE PRECISION,
    avg_temp_jul DOUBLE PRECISION,
    avg_temp_aug DOUBLE PRECISION,
    avg_temp_sep DOUBLE PRECISION,
    avg_temp_oct DOUBLE PRECISION,
    avg_temp_nov DOUBLE PRECISION,
    avg_temp_dec DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS public.dim_climate(
    climate_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    city varchar(25) NOT NULL,
    country varchar(25) NOT NULL ,
    avg_temp_jan DOUBLE PRECISION,
    avg_temp_feb DOUBLE PRECISION,
    avg_temp_mar DOUBLE PRECISION,
    avg_temp_apr DOUBLE PRECISION,
    avg_temp_may DOUBLE PRECISION,
    avg_temp_jun DOUBLE PRECISION,
    avg_temp_jul DOUBLE PRECISION,
    avg_temp_aug DOUBLE PRECISION,
    avg_temp_sep DOUBLE PRECISION,
    avg_temp_oct DOUBLE PRECISION,
    avg_temp_nov DOUBLE PRECISION,
    avg_temp_dec DOUBLE PRECISION,
    CONSTRAINT pk_climate_id PRIMARY KEY (climate_id)
);

CREATE TABLE IF NOT EXISTS public.staging_city(
    city varchar(256)  ,
    state_code varchar(3) ,
    state varchar(256) ,
    median_age DOUBLE PRECISION,
    male_population int4,
    female_population int4,
    total_population int4,
    number_of_veterans int4,
    foreign_born int4,
    average_household_size DOUBLE PRECISION,
    AmericanIndianandAlaskanNativePopulation int8,
    AsianPopulation int8,
    BlackorAfricanAmericanPopulation int8,
    HispanicorLatinoPopulation int8,
    WhitePopulation int8
);

CREATE TABLE IF NOT EXISTS public.dim_city(
    city_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    city varchar(256) NOT NULL ,
    state_id int4 NOT NULL,
    median_age DOUBLE PRECISION,
    male_population int4,
    female_population int4,
    total_population int4,
    number_of_veterans int4,
    foreign_born int4,
    average_household_size DOUBLE PRECISION,
    americanindian_and_alaskannative_population int8,
    asian_population int8,
    black_or_africanamerican_population int8,
    hispanic_or_latino_population int8,
    white_population int8,
    CONSTRAINT pk_city_id PRIMARY KEY (city_id)
);

CREATE TABLE IF NOT EXISTS public.staging_state (
    state_code varchar(3),
    state_name varchar(256)
);

CREATE TABLE IF NOT EXISTS public.dim_state (
    state_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    state_code varchar(3),
    state_name varchar(256),
    CONSTRAINT dim_state_pkey PRIMARY KEY (state_id)
);

CREATE TABLE IF NOT EXISTS public.staging_immigration(
    immigration_id int8,
    immigration_year int4,
    immigration_month int4,
    citizenship_country_code int4,
    residency_country_code int4,
    port_code varchar(3),
    arrival_date date,
    travel_mode int4,
    current_state_code varchar(3),
    departure_date date,
    immigration_age int4,
    visa_cat_code int4,
    match_flag varchar(1),
    birth_year int4,
    gender char(1),
    airline_code varchar(10),
    admission_num int8,
    visa_type varchar(10)
);

CREATE TABLE IF NOT EXISTS public.fact_immigration(
    immigration_id int8,
    immigration_year int4,
    immigration_month int4,
    citizenship_country_id int4,
    residency_country_id int4,
    port_id int4,
    arrival_date_id int4,
    travel_mode_id int4,
    current_state_id int4,
    departure_date_id int4,
    immigration_age int4,
    visa_cat_id int4,
    match_flag varchar(1),
    birth_year int4,
    gender char(1),
    airline_code varchar(10),
    admission_num int8,
    visa_type varchar(10)
);



