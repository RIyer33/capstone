# Do all imports and installs here
import configparser
import os
import datetime as dt

import dataMappings

from datetime import timedelta
from pyspark.sql import SparkSession
#from pyspark.sql import SQLContext

from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import dayofweek, monotonically_increasing_id, from_unixtime
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType,DateType, FloatType

from helperFunctions import split_column
from dataMappings import (visa_codes, mode_codes, cit_and_res_codes, ports_codes, state_codes)


#user defined functions
split_to_string = udf(split_column, StringType())
split_to_float = udf(split_column,FloatType())

#get config data by reading from dl.cfg
config = configparser.ConfigParser()
config.read('/home/workspace/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

#create the spark session
def create_spark_session():
    """Return a SparkSession object."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_airports_data(spark, output_data):
    """
    Description:
    Process the airport codes csv data, and write as parquet file in S3
    
    Arguments:
    spark       -- SparkSession object
    output_data -- path to write out the parquet files
    
    Returns - None
    
    """
    
    #read the airports codes csv file
    a_df = spark.read.options(delimiter=",").csv("data/airport-codes_csv.csv",header=True)
    
    # add new columns based on existing dataframe columns
    a_df = a_df.withColumn("latitude", split_to_float(a_df.coordinates,lit(","),lit(0),lit("float"))) \
    .withColumn("longitude",split_to_float(a_df.coordinates,lit(","),lit(1),lit("float"))) \
    .withColumn("region",split_to_string(a_df.iso_region,lit("-"),lit(1),lit("str")))
    
    #drop duplicates 
    a_df = a_df.dropDuplicates(subset=["iata_code","type"])
    
    #Gather required columns for the airports table
    a_df.createOrReplaceTempView("AirportTable")
    aSQL = spark.sql("select distinct  \
                         ident AS airport_code, \
                         type AS airport_type, \
                         name AS airport_name, \
                         double(elevation_ft) elevation_ft, \
                         continent, \
                         iso_country, \
                         region, \
                         municipality, \
                         gps_code, \
                         iata_code, \
                         local_code, \
                         latitude, \
                         longitude \
                 from AirportTable \
                 WHERE iata_code is not null and iata_code <> '0' \
                 and type <> 'closed'  \
                ")
    
    #write to parquet
    aSQL.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/airports.parquet'))

def process_climate_data(spark,output_data):
    """
    Description:
    Process World temperature data. Filtering the datasets by US cities only
    and storing the average temperature of each city by month in a parquet format file on S3.
    
    Arguments:
    spark       -- SparkSession object
    output_data -- path to write out the parquet files
    
    Returns: -- None
    
    """    
    #Read the csv file
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    temp_df = spark.read.options(delimiter=",").csv(fname,header=True)
    
    # Filter by only dates greater than or equal to year 2000 and for cities in United States Only
    temp_df.createOrReplaceTempView("CityTempTable")
    tSQL = spark.sql('''
                     select date(dt) DateRec ,
                            date_format(date(dt),'MMM') Month,
                            float(AverageTemperature) AvgTemp,
                            City, 
                            Country
                     from CityTempTable where to_date(dt,'yyyy-MM-dd') >= '2000-01-01' 
                     and Country = 'United States' 
                  ''')

    #now get the average temperature per month for each City across the years!
    # Pivot the dataframe by Month to get average temp in a single row for each row 
    t_pivot_df = tSQL.groupBy("City","Country").pivot("Month").avg("AvgTemp")

    #join the two datasets to create a single combined final dataset and drop duplicates
    f_df = tSQL.join(t_pivot_df,["City","Country"]).drop("DateRec","AvgTemp","Month").dropDuplicates()

    # Create final view  with renamed columns which will be written to parquet
    f_df.createOrReplaceTempView("FinalCityTempTable")
    fSQL = spark.sql('''
                  select City,  
                         Country, 
                         Jan AvgTempJan,
                         Feb AvgTempFeb,
                         Mar AvgTempMar,
                         Apr AvgTempApr,
                         May AvgTempMay,
                         Jun AvgTempJun,
                         Jul AvgTempJul,
                         Aug AvgTempAug,
                         Sep AvgTempSep,
                         Oct AvgTempOct,
                         Nov AvgTempNov,
                         Dec AvgTempDec
                 from FinalCityTempTable
                 ''')
    # write to parquet
    fSQL.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/climate.parquet'))


def process_demographic_data(spark, output_data):
    """
    Description:
    Process the US Cities Demographics data storing the csv as parquet file in S3
    
    Arguments:
    spark       -- SparkSession object
    output_data -- path to write out the dimesion tables
    
    Returns: -- None
    
    """
    
    # get filepath to us cities demographics data file
    df = spark.read.options(delimiter=";").csv("data/us-cities-demographics.csv",header=True)
    #Create Temporary view on dataframe
    df.createOrReplaceTempView("CityTable")
    #Rename columms and Change datatypes
    dSQL = spark.sql("Select City, \
                         State, \
                         float(`Median Age`) MedianAge, \
                         int(`Male Population`) MalePopulation, \
                         int(`Female Population`) FemalePopulation , \
                         int(`Total Population`) TotalPopulation, \
                         int(`Number of Veterans`) NumberOfVeterans, \
                         int(`Foreign-Born`) ForeignBorn, \
                         float(`Average Household Size`) AverageHouseholdSize, \
                         `State Code` StateCode, \
                         Race, \
                         int(Count) Count \
                         from CityTable")

    # Pivot the dataframe by Race to get count in a single row
    d_pivot_df = dSQL.groupBy("City","StateCode").pivot("Race").sum("Count")

    #join the two datasets to create a single combined final dataset and drop duplicates
    f_df = dSQL.join(d_pivot_df,["City","StateCode"]).drop("Race","Count").dropDuplicates()

    #rename columms 
    f_df = f_df.withColumnRenamed("American Indian and Alaska Native","AmericanIndianandAlaskanNativePopulation") \
    .withColumnRenamed("Asian","AsianPopulation") \
    .withColumnRenamed("Black or African-American","BlackorAfricanAmericanPopulation") \
    .withColumnRenamed("Hispanic or Latino","HispanicorLatinoPopulation") \
    .withColumnRenamed("White", "WhitePopulation")
    
    #write to parquet file on S3
    f_df.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/demographic.parquet'))

def process_immigration_data(spark, output_data):
    """
    Description: 
        - extracts, transforms and loads immigration data into partitioned S3 parquet file
        - extracts distinct arrival and departure dates and writes to `dates` parquet file
        
    Arguments:
        spark: Spark session
        output_data: location of S3 bucket
        
    Returns:
        None
    """

    # read immigration data
    df=spark.read.parquet("sas_data")
    # convert date fields to date type
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)

    #convert sas dates to date
    df = df.withColumn("arrival_date", get_date(df.arrdate))
    df = df.withColumn("departure_date", get_date(df.depdate))

    df = df.withColumn("arrival_date", to_date(df.arrival_date, 'yyyy-MM-dd'))
    df = df.withColumn("departure_date", to_date(df.departure_date, 'yyyy-MM-dd'))
    
    #create temp view on dataframe    
    df.createOrReplaceTempView("ImmigData")
    dSQL = spark.sql('''
                    select 
                        int(cicid) immigration_id,
                        int(i94yr) immigration_year,
                        int(i94mon) immigration_month,
                        int(i94cit) citizenship_country_code,
                        int(i94res) residency_country_code,
                        i94port port_code,
                        arrival_date,
                        int(i94mode) travel_mode,
                        i94addr current_state_code,
                        departure_date,
                        int(i94bir) age,
                        int(i94visa) visa_code,
                        matflag match_flag,
                        int(biryear) birth_year,
                        gender,
                        airline airline_code,
                        bigint(admnum) admission_num,
                        visatype visa_type,
                        int(i94yr) immigration_year_part,
                        int(i94mon) immigration_month_part
                    from ImmigData      
                    WHERE departure_date is null or year(departure_date) >= 2016
                    
                ''')

    #write to partitioned parquet files 
    dSQL.write.mode('overwrite').partitionBy("immigration_year_part","immigration_month_part").parquet(os.path.join(output_data,'capstone/immigration.parquet'))
    
    #create dates dataframe with unique dates from the file
    df.createOrReplaceTempView("DatesData")
    dtSQL = spark.sql('''
                    select distinct date
                    from 
                    (
                        select arrival_date date from DatesData WHERE arrival_date >= to_date('2016-01-01','yyyy-MM-dd')
                        UNION
                        select departure_date date from DatesData WHERE departure_date is not null 
                        and departure_date >= to_date('2016-01-01','yyyy-MM-dd')
                    ) as Drv         
                ''')

    #add columns to enrich the dates dataframe. this is will be used to write to parquet
    dtSQL = dtSQL.select("date", \
                     date_format("date", 'yyyyMMdd').cast("int").alias("date_id"), \
                     year("date").alias("year"), \
                     month("date").alias("month"), \
                     dayofmonth("date").alias("day"), \
                     weekofyear("date").alias("week"), \
                     date_format("date",'MMMM').alias("month_name"), \
                     dayofweek("date").alias("day_of_week"),\
                     date_format("date",'E').alias("day_of_week_name")
                    )

    #write to parquet files
    dtSQL.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/dates.parquet'))
    
def process_other_dimension_tables(spark,output_data):
    """
    Description:
    Process various mappings as dimensions that will be later used in the fact table
    such as visa, ports and mode of transport ,country and output in parquet format
    
    Arguments:
        spark: Spark session
        output_data: location of S3 bucket
        
    Returns:
        None
    
    """
    
    #Reading the dimensions into spark dataframe from dictionary objects
    df_visa_codes = spark.createDataFrame(list(map(list, visa_codes.items())),
                                        ["visa_cat_code","visa_category"])
    df_visa_codes = df_visa_codes.withColumn("visa_cat_code",df_visa_codes["visa_cat_code"].cast(IntegerType())) 
  
    df_ports = spark.createDataFrame(list(map(list, ports_codes.items())),
                                         ["port_code", "port_name"])
                                         
    df_travel_mode = spark.createDataFrame(list(map(list, mode_codes.items())),
                                        ["travel_mode_code","travel_code"])
    df_travel_mode = df_travel_mode.withColumn("travel_mode_code",df_travel_mode["travel_mode_code"].cast(IntegerType()))
    
    df_city_res_codes = spark.createDataFrame(list(map(list, cit_and_res_codes.items())),
                                        ["country_code","country"])
    df_city_res_codes = df_city_res_codes.withColumn("country_code",df_city_res_codes["country_code"].cast(IntegerType()))
    
    df_state_codes = spark.createDataFrame(list(map(list, state_codes.items())),
                                        ["state_code","state"])
    
    #outputting the visas, ports and mode of transport, country data
    df_visa_codes.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/visacodes.parquet'))
    
    #ports parquet
    df_ports.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/ports.parquet'))
    
    #travel mode parquet
    df_travel_mode.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/travelmode.parquet'))
    
    #country parquet
    df_city_res_codes.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/country.parquet'))
    
    #state parquet
    df_state_codes.write.mode('overwrite').parquet(os.path.join(output_data,'capstone/state.parquet'))
    
def main():
    """
    Main funtion that initiates the spark session and 
    defines location to write data to using spark
    calls the other functions that creates the various parquet files
    """
    
    spark = create_spark_session()
      
    output_data = "s3a://ri-capstone/" 
       
    process_airports_data(spark, output_data)
    process_climate_data(spark, output_data)
    process_demographic_data(spark, output_data)    
    process_immigration_data(spark, output_data) 
    process_other_dimension_tables(spark, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
