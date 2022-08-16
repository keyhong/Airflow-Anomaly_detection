import sys
from airflow.models import DAG
from datetime import datetime, timedelta
import pendulum

from werkzeug.utils import secure_filename
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint

TOKENIZER_FILE_PATH = '/lake_etl/etl/mart/anomaly_detection/tokenizer'
sys.path.append(TOKENIZER_FILE_PATH)

from log_tokenizer import LogTokenizer

## 로컬 타임존 생성
local_tz = pendulum.timezone("Asia/Seoul")

args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5, 1, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)  
}


def print_excution_date(excution_date, **kwargs):
    print('=' * 60)
    print('excution_date:', excution_date)
    print('=' * 60)
    pprint(kwargs)
    print('=' * 60)
    return 'print complete!!!'

with DAG(
    dag_id = 'STEP_9_ANOMALY_1_PREP',
    default_args = args,
    catchup = True,
    schedule_interval = "0 15 * * *" # 일배치(15시)
) as dag:
    
    tokenizer = LogTokenizer()
    tokenizer(2) #  setAttr
    
    # START
    STEP_9_ANOMALY_1_PREP_START = PythonOperator(
        task_id = 'STEP_9_ANOMALY_1_PREP_START.task1',
        python_callable = print_excution_date,
        provide_context = True,
        op_kwargs = {'excution_date': datetime.now() }
    )

    # PythonOperator
    UPTIME_LOG_PREP = PythonOperator(
        task_id = 'UPTIME_LOG_PREP.task1',
        python_callable = tokenizer.preprocess_uptime_log
    )

    # PythonOperator
    MPSTAT_LOG_PREP = PythonOperator(
        task_id = 'MPSTAT_LOG_PREP.task1',
        python_callable = tokenizer.preprocess_mpstat_log
    )

    # END 
    STEP_9_ANOMALY_1_PREP_END = DummyOperator(task_id='STEP_9_ANOMALY_1_PREP_END.task1')


    STEP_9_ANOMALY_1_PREP_START >> UPTIME_LOG_PREP >> MPSTAT_LOG_PREP >> STEP_9_ANOMALY_1_PREP_END


# documentation
dag.doc_md = __doc__

UPTIME_LOG_PREP.doc_md = """\
#### Task Documentation
주석을 처리한다
"""