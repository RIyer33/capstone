class SqlQueries:
    dim_visa_table_insert = ("""
        SELECT
            visa_cat_code as visa_cat_id,
            visa_category
        FROM staging_visa 
    """)
    
    dim_travel_mode_table_insert = ("""
          SELECT
              travel_mode_code AS travel_mode_id,
              travel_mode
           FROM staging_travel_mode   
    """)
    
    dim_country_table_insert = ("""
            SELECT 
                country_code,
                country_name
            FROM staging_country

    """)
    
    dim_ports_table_insert = ("""
           SELECT
               port_code,
               port_name
           FROM staging_ports
    """)
    
    dim_airports_table_insert = ("""
        SELECT
             airport_code,
             airport_type,
             airport_name,
             elevation_ft,
             continent,
             iso_country,
             region,
             municipality,
             gps_code,
             iata_code,
             local_code,
             latitude,
             longitude  
        FROM staging_airports
    """)
    
    dim_climate_table_insert = ("""
        SELECT 
            city,
            country,
            avg_temp_jan,
            avg_temp_feb,
            avg_temp_mar,
            avg_temp_apr,
            avg_temp_may,
            avg_temp_jun,
            avg_temp_jul,
            avg_temp_aug,
            avg_temp_sep,
            avg_temp_oct,
            avg_temp_nov,
            avg_temp_dec     
        FROM staging_climate
    """)
    
    dim_city_table_insert = ("""
        SELECT
            city,
            s.state_id,
            median_age,
            male_population,
            female_population,
            total_population,
            number_of_veterans,
            foreign_born,
            average_household_size,
            AmericanIndianandAlaskanNativePopulation,
            AsianPopulation,
            BlackorAfricanAmericanPopulation,
            HispanicorLatinoPopulation,
            WhitePopulation       
        FROM staging_city as c
        join dim_state as s
        on c.state_code = s.state_code

    """)
    
    dim_state_table_insert = ("""
        SELECT
            state_code,
            state_name       
        FROM staging_state
    """)
    
    dim_date_table_insert = ("""
        SELECT
            CAST(date_id as int) as date_id,
            date,
            year,
            month,
            day,
            week,
            month_name,
            day_of_week,
            day_of_week_name
        FROM staging_date
        WHERE date_id IS NOT NULL
    """)
    
    fact_immigration_table_insert = ("""
        Select 
            i.immigration_id,
            i.immigration_year,
            i.immigration_month,
            c.country_id as citizenship_country_id,
            rc.country_id as residency_country_id,
            p.port_id ,
            ad.date_id as arrival_date_id,
            tm.travel_mode_id,
            cs.state_id as current_state_id,
            dd.date_id as departure_date_id,
            i.immigration_age,
            v.visa_cat_id,
            i.match_flag,
            i.birth_year,
            i.gender,
            i.airline_code,
            i.admission_num,
            i.visa_type 
    from staging_immigration as i
    left join dim_country as c ON i.citizenship_country_code = c.country_code 
    left join dim_country as rc on i.residency_country_code = rc.country_code
    left join dim_ports as p on i.port_code = p.port_code 
    left join dim_date as ad on i.arrival_date = ad.date
    left join dim_travel_mode as tm on i.travel_mode = tm.travel_mode_id
    left join dim_state as cs on i.current_state_code = cs.state_code
    left join dim_date as dd on i.departure_date = dd.date 
    left join dim_visa as v on i.visa_cat_code = v.visa_cat_id 
   """)
    
 