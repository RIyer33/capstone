import datetime
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,LoadDimensionOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

dag = DAG('capstone_dag',
          #default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None, #'0 * * * *',
          start_date=datetime.datetime(2022, 1, 1, 0, 0, 0, 0)
          #max_active_runs=1
         )

start_task = DummyOperator(task_id='Begin_execution',  dag=dag)

clear_staging_tables_task = PostgresOperator(
    task_id="clear_staging_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="clear_staging_tables.sql"
)


stage_visa_codes_to_redshift = StageToRedshiftOperator(
    task_id="Stage_visa_code",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_visa",
    s3_bucket="ri-capstone/capstone",
    s3_key="visacodes.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_travel_mode_to_redshift = StageToRedshiftOperator(
    task_id="Stage_travel_mode",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_travel_mode",
    s3_bucket="ri-capstone/capstone",
    s3_key="travelmode.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_country_to_redshift = StageToRedshiftOperator(
    task_id="Stage_country",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_country",
    s3_bucket="ri-capstone/capstone",
    s3_key="country.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_ports_to_redshift = StageToRedshiftOperator(
    task_id="Stage_ports",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_ports",
    s3_bucket="ri-capstone/capstone",
    s3_key="ports.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)


stage_climate_to_redshift = StageToRedshiftOperator(
    task_id="Stage_climate",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_climate",
    s3_bucket="ri-capstone/capstone",
    s3_key="climate.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_city_to_redshift = StageToRedshiftOperator(
    task_id="Stage_city",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_city",
    s3_bucket="ri-capstone/capstone",
    s3_key="demographic.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_state_to_redshift = StageToRedshiftOperator(
    task_id="Stage_state",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_state",
    s3_bucket="ri-capstone/capstone",
    s3_key="state.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_date_to_redshift = StageToRedshiftOperator(
    task_id="Stage_date",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_date",
    s3_bucket="ri-capstone/capstone",
    s3_key="dates.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id="Stage_immigration",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="public.staging_immigration",
    s3_bucket="ri-capstone/capstone",
    s3_key="immigration.parquet/",
    #role_arn='',
    aws_region="us-west-2"
)

load_visa_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_visa_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_visa",
    columns="(visa_cat_id,visa_category)",
    sql=SqlQueries.dim_visa_table_insert,
    truncate=True
)

load_travel_mode_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_travel_mode_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_travel_mode",
    columns="(travel_mode_id,travel_mode)",
    sql=SqlQueries.dim_travel_mode_table_insert,
    truncate=True
)

load_country_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_country_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_country",
    columns="(country_code,country_name)",
    sql=SqlQueries.dim_country_table_insert,
    truncate=True
)

load_ports_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_ports_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_ports",
    columns="(port_code,port_name)",
    sql=SqlQueries.dim_ports_table_insert,
    truncate=True
)

load_climate_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_climate_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_climate",
    columns="(city,country,avg_temp_jan,avg_temp_feb,avg_temp_mar,avg_temp_apr,avg_temp_may,avg_temp_jun,avg_temp_jul,avg_temp_aug,avg_temp_sep,avg_temp_oct,avg_temp_nov,avg_temp_dec)",
    sql=SqlQueries.dim_climate_table_insert,
    truncate=True
)

load_city_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_city_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_city",
    columns="(city,state_id,median_age,male_population,female_population,total_population,number_of_veterans,foreign_born,average_household_size,americanindian_and_alaskannative_population,asian_population,black_or_africanamerican_population,hispanic_or_latino_population,white_population)",
    sql=SqlQueries.dim_city_table_insert,
    truncate=True
)

load_state_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_state_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_state",
    columns = "(state_code,state_name)",
    sql=SqlQueries.dim_state_table_insert,
    truncate=True
)

load_date_dimension_table = LoadDimensionOperator(
    task_id="Load_dim_date_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.dim_date",
    columns = "(date_id,date,year,month,day,week,month_name,day_of_week,day_of_week_name)",
    sql=SqlQueries.dim_date_table_insert,
    truncate=True
)

load_fact_immigration_table = LoadFactOperator(
    task_id="Load_fact_immigration",
    dag=dag,
    redshift_conn_id="redshift",
    target_db="dev",
    destination_table="public.fact_immigration",
    columns = "(immigration_id,immigration_year,immigration_month,citizenship_country_id,residency_country_id,port_id,arrival_date_id,travel_mode_id,current_state_id,departure_date_id,immigration_age,visa_cat_id,match_flag,birth_year,gender,airline_code,admission_num,visa_type)",
    sql=SqlQueries.fact_immigration_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.dim_visa", "public.dim_travel_mode", "public.dim_country", "public.dim_ports", "public.dim_state","public.dim_city","public.dim_date","public.fact_immigration"]
)

end_task = DummyOperator(task_id='Stop_execution',  dag=dag)

start_task >> clear_staging_tables_task 
clear_staging_tables_task >> stage_visa_codes_to_redshift >> load_visa_dimension_table 
clear_staging_tables_task >> stage_travel_mode_to_redshift >> load_travel_mode_dimension_table
clear_staging_tables_task >> stage_country_to_redshift >> load_country_dimension_table
clear_staging_tables_task >> stage_ports_to_redshift >> load_ports_dimension_table
clear_staging_tables_task >> stage_climate_to_redshift >> load_climate_dimension_table
clear_staging_tables_task >> [stage_state_to_redshift , stage_city_to_redshift ]
stage_state_to_redshift >>  load_state_dimension_table 
[stage_city_to_redshift, load_state_dimension_table] >> load_city_dimension_table
clear_staging_tables_task >> stage_date_to_redshift >> load_date_dimension_table
clear_staging_tables_task >> stage_immigration_to_redshift
load_visa_dimension_table >> load_fact_immigration_table 
load_travel_mode_dimension_table >> load_fact_immigration_table
load_country_dimension_table >> load_fact_immigration_table
load_ports_dimension_table >> load_fact_immigration_table
load_climate_dimension_table >> load_fact_immigration_table
load_state_dimension_table >> load_fact_immigration_table
load_city_dimension_table >> load_fact_immigration_table
load_date_dimension_table >> load_fact_immigration_table
stage_immigration_to_redshift >> load_fact_immigration_table >> run_quality_checks >> end_task