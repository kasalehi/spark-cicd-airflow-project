from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import argparse

#  lets define main fumction for parameters

def main(env,bq_project,bq_dataset,bq_table):
    try:
        # Create SparkSession
        spark = SparkSession.builder.appName("Spark App").getOrCreate()
        # Load data from BigQuery
        input_data=f"gs://keyvan/source-dev"
        data=spark.read.csv(input_data,header=True,inferSchema=True)
        #write data on bigquery table
        data.write.format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{bq_table}") \
            .option("writeMethod","direct") \
            .mode("overwrite") \
            .save()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()
        
        
        
        
        
        
if __name__=="__main__":
    parser=argparse.ArgumentParser(description="airflow parsere")
    parser.add_argument("--env",type=str,required=True,help="env")
    parser.add_argument("--bq_project",type=str,required=True,help="bq_project")
    parser.add_argument("--bq_dataset",type=str,required=True,help="bq_dataset")
    parser.add_argument("--bq_table",type=str,required=True,help="bq_tabl")
    args=parser.parse_args()
    main(args.env,args.bq_project,args.bq_dataset,args.bq_table)
                                   
                                   
                                   
     
        

