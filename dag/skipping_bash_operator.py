# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class SkippingBashOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :type bash_command: str
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str
    """
    template_fields = ('bash_command', 'env')

    template_ext = ('.sh', '.bash',)

    ui_color = '#f0ede4'


    @apply_defaults
    def __init__(
            self,
            bash_command,
            db_connector=None, # 추가 구현
            err_tasks = None, # 추가 구현            
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding

    def execute(self, context):
        super(SkippingBashOperator, self).execute(context)

    def on_kill(self, context):
        super(SkippingBashOperator, self).on_kill()


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

        ''' 에러 태스크와의 인과관계 파악 (comment_preprocessor) '''
        
        import comment_preprocessor
        
        TaskRelationAnalizer().comment_prepreocess()

        tmp_type = _func1
        
        if task_name in self.err_tasks:

            if tmp_type:
                 raise AirflowException('에러가 있는 태스크입니다')


    def _find_cmd_prog(self):
        
        matching_prog = {
            'DW' : 'Hive',
            'DM' : 'Hive',
            


        }

        self.bash_command.split(' ')[0].split('/')[-1].split('_')[1]