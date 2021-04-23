from datetime import timedelta
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo.errors import ConnectionFailure
from pymongo.command_cursor import CommandCursor

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'mongo2',
    default_args=default_args,
    description='A simple mongo DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

# t1, t2 and t3 are examples of tasks created by instantiating operators


rollup = [
    {
        '$group': {
            '_id': {
                'DO': '$DOLocationID',
                'PU': '$PULocationID',
                'D': '$trip_startdate_isodate'
            },
            'count': {
                '$sum': 1
            }
        }
    }, 
    {
        '$out': {
            's3': {
                'bucket': 'chuck.kalmanek',
                'region': 'us-east-1',
                'filename': {
                    '$concat': [
                        'out/',
                        {'$toString': '$_id.D'
                        },
                        '/'
                    ]
                },
                'format': {
                    'name': "json"
                }
            }
        }
    }
]

def run_agg(aggr=[], col='nyc-yellow-cab-trips', db='sample'):
    client = MongoHook('datalake_default').get_conn()
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
    except ConnectionFailure:
        print("Server not available")

    cursor = MongoHook('datalake_default').aggregate(mongo_collection=col,
                                          aggregate_query=aggr, mongo_db=db)
    cid = cursor.cursor_id
    print(f'Wrote data out from DataLake: cursorid = {cid: 12d}')

t1 = PythonOperator(task_id="run_agg",
                    op_kwargs={'aggr': rollup},
                    python_callable=run_agg, retries=1, dag=dag,)


dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
dropoff = [
    {
        '$group': {
            '_id': {
                'DO': '$_id.DO',
            },
            'count': {
                '$sum': 1
            }
        }
    },
    {
        '$out': {
            's3': {
                'bucket': 'chuck.kalmanek',
                'region': 'us-east-1',
                'filename': {
                    '$concat': [
                        'out/do=',
                        {'$toString': '$_id.DO'
                         },
                        '/'
                    ]
                },
                'format': {
                    'name': "json"
                }
            }
        }
    }
]

t2 = PythonOperator(task_id="run_dropoff",
                    op_kwargs={'aggr': dropoff, 'db': 'datasets', 'col': 'rollups'},
                    python_callable=run_agg, retries=1, dag=dag,)

t1 >> t2
