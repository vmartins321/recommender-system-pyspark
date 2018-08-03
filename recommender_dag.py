from datetime import timedelta, datetime
import json

import airflow
from airflow.models import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator


default_args = {
	'owner': 'victoria',
	'depends_on_past': False,
	'start_date': datetime(2017, 12, 4), 
	'email': ['victoria@gmail.com'],
	'email_on_failure': True,
	'email_on_retry': True,
	'retries': 5,
	'retry_delay': timedelta(minutes=5),
}

schedule_interval = "@daily"

dag = DAG('store_recs_dag_v7', default_args=default_args, schedule_interval=schedule_interval)

t1 = BigQueryOperator(
	task_id='bq_write_store_recs_train',
	use_legacy_sql=False,
	write_disposition='WRITE_TRUNCATE',
	allow_large_results=True,
	destination_dataset_table='data_science.store_recs_train',
	delegate_to='victoria@gmail.com',
	dag=dag,
	bql='''
	#standardSQL
	WITH 
	transactions AS (
	SELECT user_id AS user
	, CAST(store.id AS INT64) AS store_id
	FROM `store_table`
	WHERE PARSE_DATE("%Y%m%d", _TABLE_SUFFIX)
	  BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY),DAY)
	  AND DATE_TRUNC(CURRENT_DATE(),DAY)
	), 

	users AS (
	SELECT user
	, COUNT(*) AS c
	FROM transactions 
	GROUP BY user HAVING c > 5
	),

	stores AS (
	SELECT id
	FROM `stores`
	WHERE id IS NOT NULL
	AND  supportStage = 'SUPPORTED_CHECKED')

	SELECT user
	, store_id
	, CASE WHEN store_id IS NOT NULL THEN 1 ELSE NULL END AS rating
	FROM transactions
	JOIN stores
	ON transactions.store_id=stores.id
	WHERE user IN (SELECT user FROM users)
	AND store_id IN (SELECT id FROM stores)
	GROUP BY user, store_id
	''')

t2 = BigQueryToCloudStorageOperator(
	task_id='bq_to_gcs_store_recs_train',
	source_project_dataset_table='data_science.store_recs_train',
	destination_cloud_storage_uris=['gs://store_recs/development/training/{{ ds }}/store_recs_train_*.json'],
	export_format='NEWLINE_DELIMITED_JSON',
	delegate_to='victoria@gmail.com',
	dag=dag
	)

t3 = DataprocClusterCreateOperator(
	task_id='create_cluster',
	project_id='my_project',
	cluster_name='cluster-1',
	num_workers=25,
	storage_bucket='data-science',
	init_actions_uris=['gs://initialization-actions/create-my-cluster.sh'],
	properties= {"spark:spark.executor.instances": "74", 
				 "spark:spark.yarn.executor.memoryOverhead": "2048",
				 "spark:spark.yarn.driver.memoryOverhead": "3072",
				 "spark:spark.executor.cores": "5",
				 "spark:spark.driver.cores": "14",
				 "spark:spark.default.parallelism": "800",
				 "yarn:yarn:scheduler.maximum-allocation-mb": "50000",
				 "yarn:yarn.nodemanager.resource.memory-mb": "50000",
				 "spark:spark.driver.maxResultsSize": "55g",
				 "spark:spark.driver.memory": "19g",
				 "spark:spark.executor.memory": "19g",
				 "spark:spark.executor.extraJavaOptions": "-XX:+PrintGCDetails"},
	master_machine_type='n1-standard-16',
	worker_machine_type='n1-standard-16',
	zone='us-central1-b',
	gcp_conn_id='google-cloud-default',
	service_account='my_account@iam.gserviceaccount.com',
	service_account_scopes=['https://www.googleapis.com/auth/cloud-platform'],
	delegate_to='victoria@gmail.com',
	dag=dag)

t4 = DataProcPySparkOperator(
	task_id='submit_job',
	main='gs://store_recs/development/model/spark_store_recs_v2.py',
	cluster_name='cluster-1',
	dag=dag
	)

t5 = DataprocClusterDeleteOperator(
	task_id='delete_cluster',
	cluster_name='cluster-1',
	project_id='my_project',
	dag=dag)

t2.set_upstream(t1)
t4.set_upstream([t3, t2])
t5.set_upstream(t4)