from datetime import datetime
import pendulum

from airflow.models import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import sys
sys.path.append('/lake_etl/airflow')
from skipping_python_operator import SkippingPythonOperator

from pyspark.sql import SparkSession

# from skipping_bash_operator import SkippingBashOperator
# from context import AnomalyDetectionContext
# from task_relation_analizer import TaskRelationAnalizer

def create_spark_session():
    
    return SparkSession.builder.config("spark.hadoop.hive.exec.dynamic.partition", "true") \
                               .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
                               .config("spark.sql.shuffle.partitions", 300) \
                               .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                               .config("spark.driver.memory", "2g") \
                               .config("spark.executor.memory", "5g") \
                               .config("spark.shuffle.consolidateFiles", "true") \
                               .enableHiveSupport() \
                               .getOrCreate()


spark_session = create_spark_session
# 파이썬 테스트 함수 정의
def exe_test(excution_date, dag):
    
    print('실행 완료했습니다')

    print('excution_date :', excution_date)
    print('dag :', dag)

# [instantiate_dag]
with DAG(
    dag_id='ANOMALY',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2021, 9, 16, tzinfo=pendulum.timezone("Asia/Seoul"))
    },
    catchup=False,
    schedule_interval=None
    ) as dag:
    
    #######################################
    
    STEP_9_2_TEST_START = DummyOperator(
        task_id='STEP_9_2_TEST_START.task1'
    )

    #######################################
    
    ANOMALY_TEST = SkippingPythonOperator(
        task_id = 'ANOMALY_TEST.task1',
        python_callable = exe_test,
        db_connector = spark_session,
        provide_context = True,
        op_kwargs = {
            'excution_date': "{{ ds }}",
            'dag' : dag
        }
    )
    
    #######################################

    STEP_9_2_TEST_END = DummyOperator(
        task_id='STEP_9_2_TEST_END.task1'
    )
    
    # STEP_9_2_TEST_START >> ANOMALY_TEST >> STEP_9_2_TEST_END
    STEP_9_2_TEST_START >> ANOMALY_TEST >> STEP_9_2_TEST_END