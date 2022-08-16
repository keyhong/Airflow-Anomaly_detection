from __future__ import annotations

import shlex
from datetime import datetime
import logging
import os
import sys

import pandas as pd
import numpy as np

import subprocess

from typing import Dict, List, Tuple, Optional, Final, Union, TypeVar
Pandas_DataFrame = TypeVar(pd.DataFrame)

from error_collector import *
from abc import ABCMeta, abstractmethod
from enum import Enum, auto

from error_collector import *

logging.basicConfig(
    format = '%(asctime)s:%(levelname)s:%(message)s',
    datefmt = '%m/%d/%Y %I:%M:%S %p',
    level = logging.INFO
)


'''
def signleton(cls_):

    _instances = {}

    def get_instance(*args, **kwargs):

        if cls_ not in _instances:
            instances[cls_] = cls_(*args, **kwargs)
        return instances[cls_]

    return get_instance()
'''


log = logging.getLogger(__name__)

base_path = os.path.expanduser(os.environ.get('ETL_HOME', '/mapr/mapr.daegu.go.kr/ETL'))

AIRFLOW_LOG_FOLDER = os.path.join(os.path.expanduser(os.environ.get('AIRFLOW_HOME', '/lake_etl/airflow')), 'logs')
UPTIME_LOG_FOLDER = os.path.join(base_path, 'uptime_log')
MPSTAT_PREP_FOLDER = os.path.join(base_path, 'mpstat_prep')
MPSTAT_LOG_FOLDER = os.path.join(base_path, 'mpstat_log')
UPTIME_PREP_FOLDER = os.path.join(base_path, 'uptime_prep')

# '/mapr/mapr.daegu.go.kr/ETL/lake_etl/airflow/logs'


'''
def main(excution_date: Union[int, str]):

    obj = AirflowErrorTaskFinder(datetime=excution_date, dag_id=os.environ["AIRFLOW_CTX_DAG_ID"])

    obj.find_error_task()

    if obj.find_error_task():
        sys.exit(main())
    else:
       obj

    # 1번 파일 이름 넘기기 : __file__
    # 2번 dag 이름 넘기기 : os.environ["AIRFLOW_CTX_DAG_ID"]

    # task_nm = os.environ["AIRFLOW_CTX_TASK_ID"].split('.')[0]


    error_finder = AirflowErrorTaskFinder('20220117', 'STEP_2_DW_1_DIL_DBIG')
    error_finder.find_error_task()

    error_tracer = ErrorTracerCreator().create_error_tracer(error_finder)
    error_tracer.error_trace()
'''


class AirflowErrorTaskFinder:

    # __error_sqoop_path = f"{os.environ['ETL_HOME']}/log/lake_log/mart"

    def __new__(cls, *args, **kwargs):

        DW_ERROR_FOLDER: Final[str] = f"{os.environ['ETL_HOME']}/log/lake_log/dw"
        DM_ERROR_FOLDER: Final[str] = f"{os.environ['ETL_HOME']}/log/lake_log/mart"

        cls.meta_data: Dict[str, Dict[str, str]] = {
            "STEP_1_SQOOP_1_DIL" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Sqoop',
                "prefix_str": "/",
                "postfix_str": ".sqoop"
            },
            "STEP_1_SQOOP_1_DIL_DBIG" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Sqoop',
                "prefix_str": "/",
                "postfix_str": ".sqoop"
            },
            "STEP_1_SQOOP_2_DCN" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Sqoop',
                "prefix_str": "/",
                "postfix_str": ".sqoop"
            },
            "STEP_2_DW_1_DIL" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_2_DW_1_DIL_ATTR" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_2_DW_1_DIL_DBIG" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_2_DW_1_DIL_DBIG_ATTR" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_2_DW_2_DCN" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_3_MART_1_ANLS" : {
                "error_log_path": DM_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_3_MART_2_CDA_PPRCS" : {
                "error_log_path": DM_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_3_MART_2_CDA_LRN" : {
                "error_log_path": DM_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_3_MART_2_CDA_OPER" : {
                "error_log_path": DM_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_4_SQOOP_1_DIL_DBIG_DCN" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Sqoop',
                "prefix_str": "/c_",
                "postfix_str": ".sqoop"
            },
            "STEP_4_SQOOP_1_DIL_DCN" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Sqoop',
                "prefix_str": "/",
                "postfix_str": ".sqoop"
            },
            "STEP_4_SQOOP_2_ANLS_DCN" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Sqoop',
                "prefix_str": "/",
                "postfix_str": ".sqoop"
            },
            "STEP_5_SOSS_1_OPER" : {
                "error_log_path": DM_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_5_SOSS_2_LRN" : {
                "error_log_path": DM_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            },
            "STEP_7_META" : {
                "error_log_path": DW_ERROR_FOLDER,
                "execute_program" : 'Hive',
                "prefix_str": "/c_",
                "postfix_str": "_D_01"
            }
        }

        return super(AirflowErrorTaskFinder, cls).__new__(cls)

    def __init__(
        self,
        datetime: Union[int, str],
        dag_id: str,
        *args, **kwargs):

        # datetime 검증
        if datetime is None:
            datetime = datetime.now().strftime('%Y%m%d')
        else:
            self.datetime = datetime

        # dag_id 검증
        if dag_id is not None:

            '''
            from airflow.models import DagBag
            dag_ids = DagBag(include_examples=False).dag_ids
            '''

            dag_ids = os.listdir('./airflow_dags') # 임시
            dag_ids = [ id.split('.')[0] for id in dag_ids ] # 임시

            if dag_id not in dag_ids:
                raise Exception('입력한 DAG ID는 존재하지 않습니다.')
            else:
                self.dag_id = dag_id

        self.error_log_path = self.meta_data[self.dag_id].get('error_log_path', 'None')
        self.execute_program = self.meta_data[self.dag_id].get('execute_program', 'None')


    @property
    def inheritance_vars(self):

        if self.err_tasks is None:
            raise Exception('먼저 find_error_task 함수를 실행시켜주세요.')

        inheritance_vars = {
            'datetime': self.datetime,
            'dag_id': self.dag_id,
            'err_tasks': self.err_tasks,
            'err_tasks_paths': self.err_tasks_paths
        }

        return inheritance_vars

    def find_error_task(self) -> List[str]:
        """ 서브 프로세스를 이용해 Linux에서 find 커맨드를 날려서 에러 task를 추출하는 함수 """

        command = f"/usr/bin/find {self.error_log_path}/{self.datetime} -type f -name *.err"

        process = subprocess.Popen(
            shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
        )

        output, _ = process.communicate()

        # 에러의 절대 경로를 담고 있는 pd.Series 생성
        err_abs_paths: pd.Series = pd.Series( output.decode('utf-8').split('\n')[:-1] )

        if err_abs_paths.empty:
            return {}

        if self.execute_program == 'Sqoop':
            # 이 부분 수정
            split_str = '_' + int(self.datetime) - 1
        elif self.execute_program == 'Hive' or 'PYTHON':
            split_str = self.meta_data[self.dag_id].get('postfix_str')

        err_task : List[str] = [ elements[0] for elements in err_abs_paths.str.split(split_str) ]


        #####################################################################################################################

        # 에러 task 검증
        err_task = self._isErrorTask(err_task)

        #####################################################################################################################

        # 태스크 관계성 파악
        from task_relation_analizer import TaskRelationAnalizer
        relation_analizer = TaskRelationAnalizer(dag_id=self.dag_id)

        all_dags = relation_analizer.get_all_py_dag_paths()
        relation_analizer.analyze_relation(all_dags)

        df = relation_analizer.all_df[relation_analizer.all_df.dag_id == self.dag_id]

        #####################################################################################################################

        task_list = []

        for elem in df.task_id:
            assert isinstance(elem, (list, str)), '올바르지 않은 데이터 타입입니다.'

            if isinstance(elem, list):
                task_list.extend(elem)
            elif isinstance(elem, str):
                task_list.append(elem)
        else:
            assert len(task_list) == len(set(task_list)), '사라진 리스트 항목이 있습니다.'
            dag_task_set = set(task_list)

        real_error_task = pd.Series(list(set(err_task).intersection(dag_task_set)))

        compared_str = self.meta_data[self.dag_id].get('prefix_str') + real_error_task + self.meta_data[ self.dag_id ].get('postfix_str')
        compared_str = '|'.join(compared_str)

        self.err_tasks = list(real_error_task)
        self.err_tasks_paths = sorted(err_abs_paths[err_abs_paths.str.contains(compared_str)])

    def _isErrorTask(self, err_arr: List[str]):

        # 에러 로그가 4개 이상 있는 파일 리스트만 추출
        from collections import Counter
        tmp_dict = dict( (key, value) for (key, value) in Counter(err_arr).items() if value >= 4 )

        if not tmp_dict:
            err_task = {}
        else:
            err_task = { key.split(self.meta_data[self.dag_id]['prefix_str'])[-1] for key in tmp_dict.keys() }

        return err_task

class ErrorTracerEnum(Enum):
    PythonErrorTracer = auto()
    SqoopErrorTracer = auto()
    HiveErrorTracer = auto()

class ErrorTracerCreator():

    # 팩토리 메서드 패턴을 이용하여 상속받는 하위 클래스의 인스턴스를 리턴
    def create_error_tracer(self, err_finder : AirflowErrorTaskFinder) -> ErrorTracer:

        logging.info('실행하는 프로그램은 %s입니다', {err_finder.meta_data.get(err_finder.dag_id).get('execute_program')})

        if AirflowErrorTaskFinder.meta_data.get(err_finder.dag_id).get('execute_program') == 'Sqoop':
            return SqoopErrorTracer(**err_finder.inheritance_vars)

        elif AirflowErrorTaskFinder.meta_data[err_finder.dag_id].get('execute_program') == 'Hive':
            return HiveErrorTracer(**err_finder.inheritance_vars)

        elif AirflowErrorTaskFinder.meta_data[err_finder.dag_id].get('execute_program') == 'Python':
            return PythonErrorTracer(**err_finder.inheritance_vars)

        else:
            raise Exception('일치하는 dag_id가 없습니다')

class ErrorTracer(metaclass=ABCMeta):

    def __init__(
        self,
        datetime: Union[int, str],
        dag_id: str,
        err_tasks: List[str],
        err_tasks_paths: List[str],
        base_path: Optional[str] = None,
        *args, **kwargs):

        self.datetime = datetime
        self.dag_id = dag_id
        self.err_tasks = err_tasks
        self.err_tasks_paths = err_tasks_paths

        if base_path is None:
            self.base_path = os.path.expanduser(os.environ.get('ETL_HOME', '/mapr/mapr.daegu.go.kr/ETL'))

    @abstractmethod
    def error_trace(metaclass):
        pass

    def get_readable_paths(self):

        should_read_paths = []

        self.err_tasks_paths = sorted(self.err_tasks_paths)

        for err_task in self.err_tasks:
            should_read_task = None
            
            for error_path in self.err_tasks_paths:
                if err_task in error_path:
                    should_read_task = error_path
                
                if error_path == self.err_tasks_paths[-1]:
                    should_read_paths.append(should_read_task)

        self.readable_paths = should_read_paths


class PythonErrorTracer(ErrorTracer):

    def __init__(
        self,
        datetime: Union[int, str],
        dag_id: str,
        err_tasks: List[str],
        err_tasks_paths: List[str],
        base_path: Optional[str] = None,
        *args, **kwargs):

        super(PythonErrorTracer, self).__init__(
            datetime=datetime,
            dag_id=dag_id,
            err_tasks=err_tasks,
            err_tasks_paths=err_tasks_paths,
            *args, **kwargs)

        self.err_task = err_task

        if base_path is None:
            base_path = os.path.expanduser(os.environ.get('ETL_HOME', '/mapr/mapr.daegu.go.kr/ETL'))

        self.python_parser = PythonError()

    def error_trace(self):
        pass

    def python_mem_err_trace(self) -> Pandas_DataFrame:

        if not self.err_task:
            return '종료'

        airflow_past, airflow_today = self._prepare_data_mart(days_ago=30)

        # 모델링
        from model import MeanShiftModel
        from collections import Counter

        # 모델링 결과 테이블 생성
        threshold_df = pd.DataFrame(columns=['task', 'idx', 'estimated_potint'])
        ms = MeanShiftModel()

        # 모델링 iteration
        for task in self.err_task:

            err_task_df = airflow_past[airflow_past.task == task]

            for idx in sorted(err_task_df.idx.unique()):

                if idx == 0:
                    continue

                ms.initialize_model()

                X = err_task_df[err_task_df.idx == idx].loadAverage_1
                ms.set_training_data(X)

                # meanshift 클러스터링 실행 및 실행시 에러처리
                try:
                    ms.clustering()
                except ValueError as e:

                    threshold_df = threshold_df.append({
                        'task': task,
                        'idx': idx,
                        'estimated_potint': np.around(X.values[0], 2),
                        'std': 0
                    }, ignore_index=True)
                    continue

                # 가중 평균을 계산하여 원점들의 중점을 계산
                cnt = Counter(ms.get_fitting_lables())

                weights = np.array([ cnt[c_it] for c_it in ms.get_fitting_lables() ])
                estimated_potint = np.dot(X.T, weights) / sum(weights)

                threshold_df = threshold_df.append({
                    'task': task,
                    'idx': idx,
                    'estimated_potint': np.around(estimated_potint, 2),
                    'std': np.around(X.std(), 2)
                }, ignore_index=True)

                # 모델초기화
                ms.initialize_model()
        else:
            del err_task_df
            threshold_df['low_threshold'] = threshold_df['estimated_potint'] - ( 2.5 * threshold_df['std'] )
            threshold_df['high_threshold'] = threshold_df['estimated_potint'] + ( 2.5 * threshold_df['std'] )

        # return airflow_today, threshold_df
        memory_err_df = self._find_today_err(airflow_today, threshold_df)
        return memory_err_df

    def _find_today_err(
        self,
        airflow_today: Pandas_DataFrame,
        threshold_df: Pandas_DataFrame,
        err_standard: int=15,
        ) -> Pandas_DataFrame:

        ''' 색출 '''
        # 임계값 기준을 벗어나는 task
        err_task_df = airflow_today[airflow_today.task.isin(self.err_task)]

        err_task_df = err_task_df.merge(threshold_df[['task', 'idx', 'low_threshold', 'high_threshold']], how='left', on=['task', 'idx'])
        err_task_df['anomaly_bool'] = np.where( (err_task_df['loadAverage_1'] < err_task_df['low_threshold']) | (err_task_df['loadAverage_1'] > err_task_df['high_threshold']), 1, 0)

        memory_err_df = pd.DataFrame()

        for task in self.err_task:

            continuous_cnt = 0
            init_val = err_task_df[err_task_df.task == task].idx.values[0]

            for it in range(1, len(err_task_df[err_task_df.task == task])):

                if it == len(err_task_df[err_task_df.task == task])-1:

                    if err_standard < continuous_cnt:
                        anomaly_row = err_task_df[(err_task_df.task == task) & (err_task_df.idx == init_val)]
                        anomaly_row['err_type'] = 'Memory_Error'
                        memory_err_df = memory_err_df.append(anomaly_row, ignore_index=True)
                    break

                if ( err_task_df[err_task_df.task==task].idx.values[it-1] ) == ( err_task_df[err_task_df.task==task].idx.values[it]-1 ):
                    continuous_cnt += 1
                else:
                    if err_standard < continuous_cnt:
                        anomaly_row = err_task_df[(err_task_df.task == task) & (err_task_df.idx == init_val)][['datetime', 'dag', 'task', 'idx', 'loadAverage_1']]
                        anomaly_row['err_type'] = 'Memory_Error'
                        memory_err_df = memory_err_df.append(anomaly_row, ignore_index=True)

                    continuous_cnt = 0
                    init_val = err_task_df[err_task_df.task == task].idx.values[it]

        ''' 에러 원인 분석 '''
        if memory_err_df.empty:

            anomaly_row = err_task_df[(err_task_df.task == task)]
            anomaly_row = err_task_df[(err_task_df.idx == anomaly_row.idx.max())]
            anomaly_row['err_type'] = 'Syntax_Error'
            memory_err_df = memory_err_df.append(anomaly_row, ignore_index=True)

        return memory_err_df

    def _prepare_data_mart(self, days_ago: int = 20) -> Tuple[Pandas_DataFrame, Pandas_DataFrame]:

        airflow_preprocessor = AirflowPreprocessor(airflow_log_path = AIRFLOW_LOG_FOLDER, days_ago = days_ago)
        uptime_preprocessor = UptimePreprocessor(uptime_log_path = UPTIME_LOG_FOLDER, uptime_prep_path = UPTIME_PREP_FOLDER, days_ago = days_ago)
        mpstat_preprocessor = MpstatPreprocessor(mpstat_log_path = MPSTAT_LOG_FOLDER, mpstat_prep_path = MPSTAT_PREP_FOLDER, days_ago = days_ago)

        airflow_today = airflow_preprocessor.get_today_airflow() # 예측할 데이터(오늘)
        airflow_past = airflow_preprocessor.get_std_airflow() # 학습(?)할 데이터(과거)

        # 불필요한 dags 제거
        stop_dags = ['STEP_7_META', 'STEP_9_ANOMALY_1_PREP']
        airflow_past = airflow_past[~airflow_past.dag.isin(stop_dags)]
        airflow_today = airflow_today[~airflow_today.dag.isin(stop_dags)]

        # 각 데이터프레임에 실행시간시점 부터 1초 단위로 인덱스를 부여한다.
        airflow_past = self._get_index_df(airflow_past, 'task')
        airflow_today = self._get_index_df(airflow_today, 'task')

        # 메모리 소스 데이터 가져오기
        uptime_past = uptime_preprocessor.get_std_uptime()
        mpstat_past = mpstat_preprocessor.get_std_mpstat()
        uptime_today = uptime_preprocessor.get_today_uptime()
        mpstat_today = mpstat_preprocessor.get_today_mpstat()

        uptime_total = pd.concat([uptime_past, uptime_today], axis=0)
        mpstat_total = pd.concat([mpstat_past, mpstat_today], axis=0)

        # 각 데이터프레임을 datetime을 기준으로 병합한다.
        airflow_past = airflow_past.merge(uptime_total, how='left', on='datetime')
        airflow_past = airflow_past.merge(mpstat_total, how='left', on='datetime')
        # airflow_past = airflow_past[airflow_past.loadAverage_1.notnull()]

        airflow_today = airflow_today.merge(uptime_total, how='left', on='datetime')
        airflow_today = airflow_today.merge(mpstat_total, how='left', on='datetime')
        # airflow_today = airflow_today[airflow_today.loadAverage_1.notnull()]

        # _get_init_mean
        airflow_past, init_mean_df = self._get_init_mean(airflow_past)
        airflow_today, _ = self._get_init_mean(airflow_today, init_mean_df)

        return airflow_past, airflow_today

    def _get_index_df(self, df: Pandas_DataFrame, task_col: str) -> Pandas_DataFrame:
        """인풋 데이터프레임의 일자별-dag별-task별로 1초 단위 인덱스를 순서대로 부여하는 함수

        Parameters
        ----------
        df : pandas.core.frame.DataFrame
            인덱스를 부여할 판다스 데이터프레임
        task_col : str
            에어플로우 로그에서 dag의 task가 들어있는 컬럼명

        Notes
        -----
        (def) add_index_to_df : datetime의 년월일을 조작하여, 일 단위 데이터프레임을 추출하여 인덱스를 부여하는 함수
        """

        def add_index_to_df(df: Pandas_DataFrame, dt_col: Optional[str] = 'datetime') -> Pandas_DataFrame:
            """인풋 데이터프레임의 일자별-dag별-task별로 1초 단위 인덱스를 순서대로 부여하는 함수

            Parameters
            ----------
            df : pandas.core.frame.DataFrame
                인덱스를 부여할 판다스 데이터프레임
            dt_col : str | None
                데이터프레임에서 datetime 데이터가 있는 컬럼명
            """

            years = df[dt_col].dt.year
            months = df[dt_col].dt.month
            days = df[dt_col].dt.day

            df['dt'] = years.astype(str) + months.astype(str) + days.astype(str)

            tmp_df_lst = []

            for unique_dt in df['dt'].unique():

                tmp_df = df[df['dt'] == unique_dt].reset_index(drop=True)
                tmp_df['idx'] = tmp_df.index
                tmp_df_lst.append(tmp_df)
            else:
                new_df = pd.concat(tmp_df_lst, ignore_index=True)
                new_df.drop('dt', axis=1, inplace=True)

            return new_df

        if not isinstance(df, pd.core.frame.DataFrame):
            raise TypeError(f'unsupported type: {type(df)}. Parameter "df" must be pandas.core.frame.DataFrame')

        if not isinstance(task_col, str):
            raise TypeError(f'unsupported type: {type(task_col)}. Parameter "task_col" must be str')
        elif task_col not in df.columns:
            raise KeyError(f'KeyError: {task_col}')

        tot_df = pd.DataFrame(columns=tuple(df.columns))

        for task in sorted(df[task_col].unique()):
            check_df = df[df.task == task].reset_index(drop=True)
            check_df = add_index_to_df(check_df)

            tot_df = tot_df.append(check_df, ignore_index=True)

        return tot_df

    def _get_init_mean(
        self,
        df: Pandas_DataFrame,
        init_mean_df: Optional[Pandas_DataFrame] = None
        ) -> Pandas_DataFrame:

        """인풋 데이터프레임의 일자별-dag별-task별로 1초 단위 인덱스를 순서대로 부여하는 함수

        Parameters
        ----------
        df : pandas.core.frame.DataFrame
            인덱스를 부여할 판다스 데이터프레임
        init_mean_df : str
            에어플로우 로그에서 dag의 task가 들어있는 컬럼명
        """

        if init_mean_df is None:
            init_mean_df = pd.DataFrame(columns=('task', 'mean'))

            for task in sorted(df['task'].unique()):
                condition = (df.task == task) & (df.idx == 0)
                tmp_df = df[condition].reset_index(drop=True)[['task', 'l   oadAverage_1']]
                tmp_df.columns = ['task', 'mean']

                tmp_df = tmp_df.groupby(by='task', as_index=False).mean()
                tmp_df['mean'] = np.around(tmp_df['mean'], 2)
                init_mean_df = init_mean_df.append(tmp_df, ignore_index=True)

        elif not isinstance(init_mean_df, pd.core.frame.DataFrame):
            raise TypeError(f'unsupported type: {type(init_mean_df)}. Parameter "init_mean_df" must be pandas.core.frame.DataFrame')

        diff_df = df[df.idx == 0].merge(init_mean_df, how='left', on='task')
        diff_df['diff_mean'] = diff_df['mean'] - diff_df['loadAverage_1']

        df = df.merge(diff_df[['datetime', 'dag', 'task', 'diff_mean']], how='left', on=['datetime', 'dag', 'task'])
        df.fillna(method='ffill', inplace=True)
        df['loadAverage_1'] = df['loadAverage_1'] + df['diff_mean']

        df = df[['datetime', 'dag', 'task', 'idx', 'loadAverage_1']]

        return df, init_mean_df

class HiveErrorTracer(ErrorTracer):

    def __init__(
        self,
        datetime: Union[int, str],
        dag_id: str,
        err_tasks: List[str],
        err_tasks_paths: List[str],
        base_path: Optional[str] = None,
        *args, **kwargs):

        super(HiveErrorTracer, self).__init__(
            datetime=datetime,
            dag_id=dag_id,
            err_tasks=err_tasks,
            err_tasks_paths=err_tasks_paths,
            *args, **kwargs)

        self.hive_parser = HiveError()

    def error_trace(self):
        
        for path in self.readable_paths:
            with open(path) as f:
                line = f.readline()


class SqoopErrorTracer(ErrorTracer):

    def __init__(
        self,
        datetime: Union[int, str],
        dag_id: str,
        err_tasks: List[str],
        err_tasks_paths: List[str],
        base_path: Optional[str] = None,
        *args, **kwargs):

        super(SqoopErrorTracer, self).__init__(
            datetime=datetime,
            dag_id=dag_id,
            err_tasks=err_tasks,
            err_tasks_paths=err_tasks_paths,
            *args, **kwargs)

        self.sqoop_parser = SqoopError()

    def error_trace(self):

        for path in self.readable_paths:
            with open(path) as f:
                
                check_lines = []
                
                for line in f:
                    if ('error' in line.lower()) or ('exception' in line.lower()):
                        check_lines.append(line.strip())
                
                exceptions = [
                    error_tracer.hive_parser.SemanticException,
                    error_tracer.hive_parser.FileNotFoundException,
                    error_tracer.hive_parser.OutOfMemoryError,
                    error_tracer.hive_parser.ExecutionError
                ]
                
                
                for line in check_lines:
                    
                    for exception in exceptions:
                        if exception in line:
                            logging.info(line)
                            logging.info(exception)
                            break

        
'''
class TaskSyntaxErrorTracer(ErrorTracer):

    def __init__(
        self,
        datetime:Optional[int, str] = None,
        dag_id:Optional[str] = None,
        base_path: str = ''
    ):

        super(PythonErrorTracer, self).__init__(*args, **kwargs)

    def python_mem_err_trace(self):
        pass
'''