from airflow import DAG
from airflow.operators import LoadDimensionOperator

def load_dimension_table_dag(
    parent_dag_name,
    child_dag_name,
    args,
    tables_and_queries,
    redshift_conn_id):

    subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args
    )
    with subdag:
        for table, query in tables_and_queries.items():
            load_dimension_table = LoadDimensionOperator(
                task_id=f'Load_{table}_dim_table',
                redshift_conn_id=redshift_conn_id,
                table={table},
                query=query,
                dag=subdag
            )

    return subdag