from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'challenge_dag',
    default_args=default_args,
    description='A DAG to extract, transform, and load product data into a summary table',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
)

extract_new_product_sales = """
    CREATE TABLE IF NOT EXISTS new_product_sales AS
    SELECT 
    product_id, 
    quantity,
    sale_date 
    FROM 
    product_sales 
    WHERE sale_date >= '{{ prev_ds }}'
    AND sale_date <= '{{ ds }}';
"""

# extracts new product sales since the last update of the product_sales_summary table.
t1 = PostgresOperator(
    task_id='extract_new_product_sales',
    sql=extract_new_product_sales,
    postgres_conn_id='batching-airflow',
    dag=dag,
)


aggregate_new_product_sales = '''
    CREATE TABLE IF NOT EXISTS aggregated_product_sales AS
    SELECT 
    product_id,
    SUM(quantity) as total_quantity
    FROM new_product_sales
    GROUP BY product_id;
'''

# aggregates the new orders by date
t2 = PostgresOperator(
    task_id='aggregate_new_product_sales',
    sql=aggregate_new_product_sales,
    postgres_conn_id='batching-airflow',
    dag=dag
)

update_product_sales_summary = """
INSERT INTO product_sales_summary (product_id, total_quantity)
SELECT 
  product_id,
  total_quantity
FROM 
  aggregated_product_sales 
ON CONFLICT (product_id) DO UPDATE
  SET total_quantity = product_sales_summary.total_quantity + EXCLUDED.total_quantity;
"""

# updates the orders_summary table with the aggregated data.
t3 = PostgresOperator(
    task_id='update_product_sales_summary',
    sql=update_product_sales_summary,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

drop_new_product_sales = '''
DROP TABLE new_product_sales;
'''

t4 = PostgresOperator(
    task_id='drop_new_product_sales',
    sql=drop_new_product_sales,
    postgres_conn_id='batching-airflow',
    dag=dag,
)

drop_aggregated_product_sales = '''
DROP TABLE aggregated_product_sales;
'''

t5 = PostgresOperator(
    task_id="drop_aggregated_product_sales",
    sql=drop_aggregated_product_sales,
    postgres_conn_id="batching-airflow",
    dag=dag,
)

t1 >> t2 >> t3 >> [t4, t5]