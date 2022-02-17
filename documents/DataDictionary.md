| fact_immigration       |                |                                                                    |
-------------------------|----------------|--------------------------------------------------------------------|
| **Column Name**        | **Data Type**  | **Description**                                                    |
| immigration_id         | integer    | primary key                                                            |
| immigration_year       | integer    | immigration year                                                       |
| immigration_month      | integer    | immigration month                                                      |
| citizenship_country_id | integer    | Foreign key to dim_country based on immigrant citizenship country code |
| residency_country_id   | integer    | Foreign key to dim_country based on immigrant residency_country_code   |
| port_id                | integer    | Foreign key to dim_ports based on immigrant arrival at US port         |
| arrival_date_id        | integer    | Foreign key to dim_date based on arrival_date to US of immigrant       |
| travel_mode_id         | integer    | Foreign key to dim_travel_mode based on travel_mode_code of immigrant  |
| current_state_id       | integer    | Foreign key to dim_state based on current state_code of immigrant      |
| departure_date_id      | integer    | Foreign key to dim_date based on departure_date from US of immigrant   |
| immigration_age        | integer    | age of immigrant                                                       |
| visa_cat_id            | integer    | Foreign key to dim_visa based on visa_cat_code of immigrant            |
| match_flag             | string     | Flag to show match between arrival and departure records               |
| birth_year             | integer    |  Birth Year                                                            |
| gender                 | string     |  Non-immigrant sex                                                     |
| airline_code           | string     |  Airline used to arrive in the US                                      |
| admission_num          | big int    |  Admission Number                                                      |
| visa_type              | string     |  class of admission admitting the non-immigrant to the US temporarily  |



 | dim_city                                    |            |                                                         |
 ----------------------------------------------|------------|---------------------------------------------------------|
 | **Column Name**                             | **Data Type**  | **Description**                                     |
 | city_id                                     | integer    | primary key                                             |
 | city                                        | string     | city name                                               |
 | state_id                                    | integer    | Foreign key to dim_state                                |
 | median_age                                  | float      | median age of population of city                        |
 | male_population                             | integer    | male population in city                                 |
 | female_population                           | integer    | female population in city                               |
 | total_population                            | integer    | total population in city                                |
 | number_of_veterans                          | integer    | number of veterans in city                              |
 | foreign_born                                | integer    | foreign_born population in city                         |
 | average_household_size                      | float      | Average household size in city                          |
 | americanindian_and_alaskannative_population | integer    | Population of AmericanIndian and AlaskanNatives in city |
 | asian_population                            | integer    | Asian population in city                                |
 | black_or_africanamerican_population         | integer    | Black or AfricanAmerican population  in city            |
 | hispanic_or_latino_population               | integer    | Hispanic or Latino population in city                   |
 | white_population                            | integer    | White population  in city                               |   


| dim_date         |            |                                      |
|------------------|------------|--------------------------------------|
| **Column Name**  | **Data Type**  | **Description**                  |
| date_id          | integer    | primary key yyyymmdd format          |
| date             | date       | yyyy-mm-dd                           |
| year             | integer    | yyyy format Example : 2016           |
| month            | integer    | mm format Range: 1 to 12             |
| day              | integer    | dd format Range: 1 to 31             |
| week             | integer    | week of year  Range: 1 to 52         |
| month_name       | string     | MMM (3 letter abb) Example: Jan, Feb |
| day_of_week      | integer    | day of the week (1 to 7)             |
| day_of_week_name | string     | name of the day Example: Saturday    |


| dim_climate  |            |                            |
|--------------|------------|----------------------------|
| **Column Name**  | **Data Type**  | **Description**    |
| climate_id   | integer    | primary key                |
| city         | string     | city name                  |
| country      | string     | country name               |
| avg_temp_jan | float      | Average Temp for January   |
| avg_temp_feb | float      | Average Temp for February  |
| avg_temp_mar | float      | Average Temp for March     |
| avg_temp_apr | float      | Average Temp for April     |
| avg_temp-may | float      | Average Temp for May       |
| avg_temp_jun | float      | Average Temp for June      |
| avg_temp_jul | float      | Average Temp for July      |
| avg_temp_aug | float      | Average Temp for August    |
| avg_temp_sep | float      | Average Temp for September |
| avg_temp_oct | float      | Average Temp for October   |
| avg_temp_nov | float      | Average Temp for November  |
| avg_temp_dec | float      | Average Temp for December  |


| dim_visa      |           |                           |
--------------- |-----------|---------------------------|
| **Column Name**   | **Data Type** | **Description**   |
| visa_cat_id   | integer   | visa category id          |
| visa_category | string    | visa category description |


| dim_travel_mode |           |                         |
------------------|-----------|-------------------------|
| **Column Name**     | **Data Type** | **Description** |
| travel_mode_id  | integer   | travel mode id          |
| travel_mode     | string    | travel mode description |

| dim_country  |            |                                |
|--------------|------------|--------------------------------|
| **Column Name**  | **Data Type**  | **Description**        |
| country_id   | integer    | primary key (generated unique) |
| country_code | integer    | i94 country code               |
| country_name | string     | i94 country name               |

| dim_ports   |            |                                |
|-------------|------------|--------------------------------|
| **Column Name** | **Data Type**  | **Description**        |
| port_id     | integer    | primary key (generated unique) |
| port_code   | integer    | i94 port code                  |
| port_name   | string     | i94 port name                  |

         

| dim_state       |                |                  |
|-----------------|----------------|------------------|
| **Column Name** | **Data Type**  | **Description**  |
| state_id        | integer        | primary key      |
| state_code      | string         | i94 state code   |
| state_name      | string         | i94 state name   |




