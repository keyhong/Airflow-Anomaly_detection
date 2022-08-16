from airflow.models import DAG
from datetime import datetime, timedelta
import pendulum

from werkzeug.utils import secure_filename
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



## 로컬 타임존 생성
local_tz = pendulum.timezone("Asia/Seoul")

analizer = TaskRelationAnalizer()
analizer.comment_prepreocess()

args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5, 1, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
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
    dag_id = 'STEP_9_ANOMALY_1_PREP',
    default_args = args,
    catchup = True,
    schedule_interval = "0 15 * * *" # 일배치(15시)
)

# START
STEP_9_ANOMALY_1_PREP_START = DummyOperator(
    task_id='STEP_9_ANOMALY_1_PREP_START.task1',
    provide_context = True,
    dag=dag,
)

FILE_PATH = '/lake_etl/etl/mart/soss/c_DM_NPA_DCR_RCP_STT_D_01.sh'

ANOMALY_DETECTION_PREP  = PythonOperator(
    task_id='ANOMALY_DETECTION_PREPR.task1',
    bash_command=f'{FILE_PATH}/log_preprocessor.py {{ tomorrow_ds_nodash }}',
    dag=dag,
)

# END 
STEP_9_ANOMALY_1_PREP_END = DummyOperator(
    task_id='STEP_9_ANOMALY_1_PREP_END.task1',
    dag=dag,
)

STEP_9_ANOMALY_1_PREP_START  >> SOSS_SAFE_INDEX_OPERATION  >>  STEP_9_ANOMALY_1_PREP_END

# documentation
dag.doc_md = __doc__

SOSS_SAFE_IDEX_OPER.doc_md = """
#### Task Documentation
주석을 처리한다
"""