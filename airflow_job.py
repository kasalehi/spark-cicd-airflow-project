from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistanceSensor
from airflow.models import Variable
from datetime import datetime, timedelta

# lets define default varaible s
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_job',
    default_args=default_args,
    description='A sample DAG',
    schedule_interval='@daily',
    catchup=False,  # Prevent backfilling
)

#fetch environment to dev
env = Variable.get("env", default_var="dev")
bq_project=Variable.get("bq_project", default_var="serene-broker-445515-g4")
gcs_bucket=Variable.get("gcs_bucket",default_var="keyvan")
bq_dataset=Variable.get("bq_dataset",default_var="key_{env}")
bq_table=Variable.get("bq_table",default_var="transform_{env}")


# lets define the task
#file_sensor
file_sensor = GCSObjectExistanceSensor(
    task_id='file_sensor',
    bucket=gcs_bucket,
    object=f"gs://keyvan/source-{env}/customer",
    poke_interval=10,
    mode="poke",
    dag=dag)

#pyspark submit task 
#lets first create batch_details for creatin serveless cluster
batch_details = {
    "pyspark_batch":{
        "main_pyhton_uri":f"gs://keyvan/spark-job/spark-job.py"},
    "runtime_config":{
        "version": "2.2"
    },
    "environment_config":{
        "service_account": "886365751370-compute@developer.gserviceaccount.com",
        "network_uri": "projects/serene-broker-445515-g4/global/networks/default",
        "subnetwork_uri": "projects/serene-broker-445515-g4/global/subnetworks/default",
    }
}    


spark_task=DataprocCreateBatchOperator(
    task_id='spark_task',
    project=bq_project,
    batch=batch_details,
    region="australia_southeast2",
    project_id="serene-broker-445515-g4",
    gcp_conn_id="google_cloud_default",
    dag=dag)



#lets order the tasks
file_sensor >> spark_task
    