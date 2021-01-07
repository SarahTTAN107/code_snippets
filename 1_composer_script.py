from airflow import models # allows access and create data in Airflow database
from airflow.contrib.operators import dataproc_operator # where operators from the community live => Cloud 															# Dataproc API
from airflow.operators import BashOperator # schedule bash commands
from airflow.utils import trigger_rule # adding trigger rules to operators - fine-grain control over 		                                       # operator's execution condition

WORDCOUNT_JAR = ('file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar')
input_file = '/home/airflow/gcs/data/rose.txt'
wordcount_args = ['wordcount', input_file, output_file] # arguments to pass in the jar file

yesterday = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1), 
	datetime.datetime.min.time())

output_file = os.path.join(models.Variable.get('gcs_bucket'), 'wordcount', datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep 

# dag folder: gntral1-my-composer-env-dffd0335-bucket/dags
default_dag_args = {
	'start_date': yesterday,
	'email_on_failure': False, 
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': datetime.timedelta(minutes=5),
	'project_id': models.Variable.get('gcp_project')
}

with models.DAG('wordcount_hadoop', 
	schedule_interval=datetime.timedelta(days=1),
	default_args=default_dag_args) as dag:
	
	# check if input file exists
	check_file_existence = BashOperator(
		task_id='check_file_existence',
		bash_command='if [! -f \"{}\"; then exit 1; fi'.format(input_file)
		)

	# create dataproc cluster
	create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
		task_id='create_dataproc_cluster',
		cluster_name='quickstart-cluster-{{ ds_nodash }}'
		num_workers=2, 
		zone=models.Variable.get('gce_zone'),
		master_machine_type='n1-standard-1',
		worker_machine_type='n1-standard-1'
		)

	# submit an Apache Hadoop Job
	run_dataproc_hadoop = dataproc_operator.DataProcHadoopOperator(
		task_id='run_dataproc_hadoop',
		main_jar=WORDCOUNT_JAR, 
		cluster_name='quickstart-cluster-{{ ds_nodash }}',
		arguments=wordcount_args
		)

	#delete dataproc_cluster 
	dataproc_operator.DatarocClusterDeleteOerator(
		task_id='delete_dataproc_cluster', 
		cluster_name='quickstart-cluster-{{ ds_nodash }}',
		trigger_rule=trigger_rule.TriggerRule.ALL_DONE
		)

	# define DAG dependencies
	check_file_existence >> create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster
