from datetime import datetime, timedelta
import pendulum

from airflow.models import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python import PythonOperator

import sys
sys.path.append('/lake_etl/airflow')
from skipping_operator import PythonOperator

'''
def _find_err_task(datetime=int | str) -> list:

    import subprocess

    datetime = '20220304'
    p = subprocess.Popen(['/usr/bin/find', f'/mapr/mapr.daegu.go.kr/ETL/log/lake_log/mart/{datetime}', '-type', 'f', '-name', '*.err'], stdout=subprocess.PIPE)
    out, err = p.communicate()

    err_arr = pd.Series(out.decode('utf-8').split('\n')[:-1])

    if err_arr.empty:
        exit(exit_code)

    err_arr = np.array( [ elements[0] for elements in err_arr.str.split('_D_01') ] )

    import collections
    tmp_dict = dict( (key, value) for (key, value) in collections.Counter(err_arr).items() if value >= 3 )

    if not tmp_dict:
        exit(exit_code)
    else:
        err_task = [ key.split('/c_')[-1] for key in tmp_dict.keys() ]

    return err_task
'''

def exe_test(excution_date, error_container):

    print('실행 완료했습니다')

    print('excution_date :', excution_date)
    print('excution_date :', excution_date)

args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 9, 16, tzinfo=pendulum.timezone("Asia/Seoul"))
}


# [instantiate_dag]
with DAG(
    dag_id='STEP_9_1_TEST',
    default_args=args,
    catchup=False,
    schedule_interval=None
) as dag:

    STEP_9_1_TEST = DummyOperator(
        task_id='STEP_9_1_TEST.task1'
    )

    TRIGGER_STEP_9_1_TEST = TriggerDagRunOperator(
        task_id="TRIGGER_STEP_9_1_TEST.task1",
        trigger_dag_id="STEP_9_2_TEST"
    )

    STEP_9_1_TEST >> TRIGGER_STEP_9_1_TEST