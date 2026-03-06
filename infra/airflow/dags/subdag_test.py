# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.snowflake_operator import SnowflakeOperator
# from airflow.operators.subdag import SubDagOperator
# from airflow.utils.dates import days_ago
# from subdags.snowflake_subdag import create_snowflake_subdag

# # Define the main DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'catchup': False
# }

# dag = DAG(
#     'snowflake_parent_dag',
#     default_args=default_args,
#     schedule_interval='@daily',
#     description='A DAG with SubDAGs for Snowflake',
# )

# # Start and End Dummy Tasks
# start = DummyOperator(task_id='start', dag=dag)
# end = DummyOperator(task_id='end', dag=dag)

# # SubDAGs for Snowflake processing
# load_data_subdag = SubDagOperator(
#     task_id='load_data',
#     subdag=create_snowflake_subdag('snowflake_parent_dag', 'load_data', default_args),
#     dag=dag,
# )

# transform_data_subdag = SubDagOperator(
#     task_id='transform_data',
#     subdag=create_snowflake_subdag('snowflake_parent_dag', 'transform_data', default_args),
#     dag=dag,
# )

# # DAG Flow
# start >> load_data_subdag >> transform_data_subdag >> end
