from airflow import DAG
from airflow.operators import LoadDimensionOperator

def get_load_dimension_table_dag(
        parent_dag_name,
        task_id,
        default_args,
        redshift_conn_id,
        tables=[],
        append_insert=[],
        select_queries=[],
        *args,
        **kwargs):

    dag = DAG(
        dag_id=f'{parent_dag_name}.{task_id}',
        default_args=default_args,
        **kwargs,
    )

    tasks = []
    for table, query, append in zip(tables, select_queries, append_insert):
        task = LoadDimensionOperator(
            task_id=f'load_{table}_dimension_table',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=table,
            append_insert=append,
            select_sql=query,
        )
        tasks.append(task)

    return dag
