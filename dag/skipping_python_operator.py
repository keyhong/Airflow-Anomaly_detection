
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException # 추가 구현

from airflow.utils.decorators import apply_defaults

from pyspark.sql import SparkSession
from pyhive import hive


class SkippingPythonOperator(PythonOperator):

    @apply_defaults
    def __init__(
        self,
        python_callable,
        db_connector=None, # 추가 구현
        op_args=None,
        op_kwargs=None,        
        provide_context=True,
        templates_dict=None,
        templates_exts=None,
        *args, **kwargs
    ):

        super(SkippingPythonOperator, self).__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args, **kwargs
        )

        self.db_connector = db_connector or None
        # self.err_tasks = err_tasks or []

        if self.db_connector is None:
            
            # session의 연결 상태를 확인하는 코드 (spark)
            with create_spark_ssession() as spark_session:
                response = spark_session.sql('SHOW TABLES')

                if response:
                    self.db_connector = spark_session
        
        self.db_connector = db_connector


    def execute(self, context):
        super(SkippingPythonOperator, self).execute(context)

    """
    def _check(self, context):

        '''Hive에서 에러 태스크 추출'''
        # spark_session를 통해 Hive에서 에러 task들을 추출하는 과정
        if isinstance(self.db_connector, pyspark.sql.SparkSession):
            dt = datetime.now().strftime('%Y%m%d')
            
            table_name = 'MIG.TEST'
            # err_date(str), err_time(str), dag(str), task(str), idx(BIGINT), load_average_1min(decimal), err_type(str)
            query = f''' SELECT * FROM {table_name} WHERE PT_STDR_DE = {dt} '''
            return_df = self.db_connector.sql(query)

            err_task_arr = return_df.task.unique()
        
        # hive_connector를 통해 Hive에서 에러 task들을 추출하는 과정
        elif isinstance(self.db_connector, hive.Connection):
            pass

        # 'task': <Task(PythonOperator): ANOMALY_TEST.task1>,
        if err_task_arr:
            return None
        
        '''airflow context에서 태스크 이름 추출'''
        import re
        repatter = re.compile('[a-zA-Z\_]*\.task1')
        
        start_idx = re.search(pattern, context['task']).span()[0]
        end_idx = re.search(pattern, context['task']).span()[1]

        task_name = context['task'][start_idx: end_idx-6]

        '''에러 태스크와의 인과관계 파악 (comment_preprocessor)'''
        
        import comment_preprocessor
        
        TaskRelationAnalizer().comment_prepreocess()

        tmp_type = _func1
        
        '''
        if task_name in self.err_tasks:

            if tmp_type:
                 raise AirflowException('에러가 있는 태스크입니다')
    """

    def _func1():
        pass


