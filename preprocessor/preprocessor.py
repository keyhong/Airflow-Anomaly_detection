######################################################
#    프로그램명    : preprocessor.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.06.06
#    파라미터      :
#    설명          :
#    변경일자      :
#    변경내역      :
######################################################

from __future__ import annotations

import os
import sys

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 6):
    raise Exception("This program requires at least Python 3.7")

import warnings
warnings.filterwarnings(action='ignore')

import pandas as pd
import numpy as np

import re

from datetime import datetime, timedelta

from abc import *
from regex_mixin import RegexMixin

from typing import TypeVar, List
Pandas_DataFrame = TypeVar('pandas.core.frame.DataFrame')
Pandas_Series = TypeVar('pandas.core.series.Series')

__all__ = ['AirflowPreprocessor', 'UptimePreprocessor', 'MpstatPreprocessor']

class PreprocessorBase(metaclass=ABCMeta):

    def __init__(
        self,
        log_path: str,
        prep_path: str | None = None,
        days_ago: int | None = None
    ):
    
        if isinstance(log_path, str):
            if self._check_path_exist(log_path):
                self.log_path = log_path
        else:
            raise TypeError(f'unsupported type: {type(log_path)}. Parameter "log_path" must be str')

 
        if isinstance(log_path, str):
            if self._check_path_exist(log_path):
                self.log_path = log_path
        else:
            raise TypeError(f'unsupported type: {type(log_path)}. Parameter "log_path" must be str')


        if prep_path is None:
            pass
        elif isinstance(prep_path, str):
            if self._check_path_exist(prep_path):
                self.prep_path = prep_path
        else:
            raise TypeError(f'unsupported type: {type(prep_path)}. Parameter "prep_path" must be str or None')


        if days_ago is None:
            self.days_ago = 30
        elif isinstance(days_ago, int):
            self.days_ago = days_ago
        else:
            raise TypeError(f'unsupported type: {type(days_ago)}. Parameter "days_ago" must be int')
        
        return self


    def _check_path_exist(self, input_path: str) -> bool:

        from pathlib import Path

        if Path(input_path).exists():
            return True
        else:
            raise ValueError(f'path "{input_path}" is not exist.')


    @abstractmethod
    def get_fileNames():
        pass


class AirflowPreprocessor(PreprocessorBase, RegexMixin):

    def __init__(
        self,
        airflow_log_path: str,
        days_ago: int | None = None
    ):
        PreprocessorBase.__init__(self, log_path=airflow_log_path, days_ago=days_ago)
        RegexMixin.__init__(self)

    def get_fileNames(self, method_name) -> List[str]:
        """
        Airflow의 directory 구조를 읽어 실행날짜를 기준으로 최근 N일(당일 포함)의 airflow dags 목록 파일들의 절대 경로를 반환한다.

        Returns
        --------
        f_lst : List[str]
           airflow dag 실행 목록이 담긴 list 변수
        """

        f_lst = list()

        if method_name == self.get_std_airflow.__name__:
            
            dag_lst = [ dag for dag in os.listdir(self.log_path) if 'STEP' in dag ]

            for dag in dag_lst:
                task_lst = [ task for task in os.listdir(f'{self.log_path}/{dag}') if not 'TRIGGER' in task ]

                for task in task_lst:
                    dt_partitions = [ dt for dt in os.listdir(f'{self.log_path}/{dag}/{task}') ]
                    dt_partitions.sort(reverse=True)

                    for dt in dt_partitions[1 : self.days_ago+1]:
                        f_name = os.listdir(f'{self.log_path}/{dag}/{task}/{dt}')[0]
                        f_lst.append(f'{self.log_path}/{dag}/{task}/{dt}/{f_name}')
            else:
                f_lst.sort()

            return f_lst

        elif method_name == self.get_today_airflow.__name__:

            dag_lst = [ dag for dag in os.listdir(self.log_path) if 'STEP' in dag ]

            for dag in dag_lst:
                task_lst = [ task for task in os.listdir(f'{self.log_path}/{dag}') if not 'TRIGGER' in task ]

                for task in task_lst:
                    dt_partitions = [ dt for dt in os.listdir(f'{self.log_path}/{dag}/{task}') ]
                    dt_partitions.sort(reverse=True)

                    f_name = os.listdir(f'{self.log_path}/{dag}/{task}/{dt_partitions[0]}')[0]
                    f_lst.append(f'{self.log_path}/{dag}/{task}/{dt_partitions[0]}/{f_name}')
            else:
                f_lst.sort()

            return f_lst
            
        else:
            return 1


    def _classify_log_dags(self, df: Pandas_DataFrame, freq: str) -> Pandas_DataFrame:
        """
        Pandas DataFrame을 input으로 받아, dag의 시작시간과 끝시간 사이의 모든 시간을 만들어준다.

        Parameters
        ----------
        df: pandas.core.frame.DataFrame
           기준이 되는 DataFrame
        freq : str
           주기 문자열은 배수를 가지를 수 있다. e.g. '5H', '5M', '5S'
        """

        df.reset_index(drop=True, inplace=True)

        result_df = pd.DataFrame(columns=['datetime', 'dag', 'task'])

        for i in range(len(df)):

            tmp_df = pd.DataFrame(columns=['datetime', 'dag', 'task'])

            start_dt = df.loc[i, 'start_dt']
            end_dt = df.loc[i, 'end_dt']

            tmp_df['datetime'] = pd.date_range(start=start_dt, end=end_dt, freq=freq)
            tmp_df['dag'] = df.loc[i, 'dag']
            tmp_df['task'] = df.loc[i, 'task']

            result_df = result_df.append(tmp_df, ignore_index=True)

        return result_df


    def get_std_airflow(self) -> Pandas_DataFrame:

        f_lst = self.get_fileNames(sys._getframe(0).f_code.co_name)

        result_df = pd.DataFrame(columns=['dag', 'task', 'start_dt', 'end_dt'])

        result_df['dag'] = pd.Series(f_lst).str.split('/').str[7]
        result_df['task'] = pd.Series(f_lst).str.split('/').str[8].str.split('.').str[0]

        start_dts: Pandas_Series = pd.Series(dtype='datetime64[ns]')
        end_dts: Pandas_Series = pd.Series(dtype='datetime64[ns]')

        for f_name in f_lst:

            with open(f_name, 'r') as f:

                text = f.readlines()

                try:
                    start_dt = self.airflowTime.findall(text[0])[0]
                    end_dt = self.airflowTime.findall(text[-1])[0]
                except IndexError as e:
                    start_dt = np.nan
                    end_dt = np.nan

                start_dts = start_dts.append(pd.Series(start_dt), ignore_index=True)
                end_dts = end_dts.append(pd.Series(end_dt), ignore_index=True)

        result_df['start_dt'] = start_dts
        result_df['end_dt'] = end_dts

        # 데이터 타입 변환
        result_df[['start_dt', 'end_dt']] = result_df[['start_dt', 'end_dt']].apply(pd.to_datetime, axis=1)

        # 결측값 제거
        result_df.dropna(inplace=True)

        # 연속된 스케줄러가 아닌 task 제외하기
        latest_date = (result_df.start_dt.max() - timedelta(days=self.days_ago))
        std_date = datetime(latest_date.year, latest_date.month, latest_date.day)
        result_df = result_df[std_date <=  result_df.start_dt].sort_values(by=['start_dt', 'end_dt']).reset_index(drop=True)

        # 분별한 dag를 start_dt와 end_dt에 따라 분리
        result_df = self._classify_log_dags(df=result_df, freq='1s')

        result_df = result_df[result_df.dag != 'STEP_9_ANOMALY_1_PREP']

        return result_df


    def get_today_airflow(self) -> Pandas_DataFrame:

        f_lst = self.get_fileNames(sys._getframe(0).f_code.co_name)

        result_df = pd.DataFrame(columns=['dag', 'task', 'start_dt', 'end_dt'])

        result_df['dag'] = pd.Series(f_lst).str.split('/').str[7]
        result_df['task'] = pd.Series(f_lst).str.split('/').str[8].str.split('.').str[0]

        start_dts: Pandas_Series = pd.Series(dtype='datetime64[ns]')
        end_dts: Pandas_Series = pd.Series(dtype='datetime64[ns]')

        for f_name in f_lst:

            with open(f_name, 'r') as f:

                text = f.readlines()

                try:
                    start_dt = self.airflowTime.findall(text[0])[0]
                    end_dt = self.airflowTime.findall(text[-1])[0]
                except IndexError as e:
                    start_dt = np.nan
                    end_dt = np.nan


                start_dts = start_dts.append(pd.Series(start_dt), ignore_index=True)
                end_dts = end_dts.append(pd.Series(end_dt), ignore_index=True)

        result_df['start_dt'] = start_dts
        result_df['end_dt'] = end_dts

        # 데이터 타입 변환
        result_df[['start_dt', 'end_dt']] = result_df[['start_dt', 'end_dt']].apply(pd.to_datetime, axis=1)

        # 결측값 제거
        result_df.dropna(inplace=True)

        # 분별한 dag를 start_dt와 end_dt에 따라 분리
        result_df = self._classify_log_dags(df=result_df, freq='1s')

        return result_df


class UptimePreprocessor(PreprocessorBase, RegexMixin):

    def __init__(
        self,
        uptime_log_path: str,
        uptime_prep_path: str,
        days_ago: int | None = None
    ):

        PreprocessorBase.__init__(self, log_path=uptime_log_path, prep_path=uptime_prep_path, days_ago=days_ago)
        RegexMixin.__init__(self)


    def get_fileNames(self, method_name: str) -> List[str] | str:
        """
        호출한 함수의 이름을 읽어, 함수의 이름 별로 읽어와야 할 파일들의 목록을 list로 반환한다.

        Parameters
        ----------
        method_name : str
           호출한 함수의 이름

        Returns
        -------
        list
           airflow dag 실행 목록이 담긴 리스트

        See Also
        --------
        self.get_std_uptime.__name__ : 실행날짜 기준 지난 N일(당일 제외)의 토크나이징된 csv 파일을 불러와 결합한 pandas.core.frame.DataFrame을 제공하는 함수
        self.get_today_uptime.__name__ : 실행날짜 기준 당일의 log파일을 토크나이징한 pandas.core.frame.DataFrame을 제공하는 함수
        """       

        if method_name == self.get_std_uptime.__name__:

            datatype = 'csv'
            path = self.prep_path

            f_lst = os.listdir(path)
            f_lst.sort(reverse=True)

            f_lst = f_lst[0:self.days_ago]
            f_lst = [ '/'.join( [path, f_name] ) for f_name in f_lst if datatype in f_name ]
            f_lst.sort()

            return f_lst

        elif method_name == self.get_today_uptime.__name__:

            datatype = 'log'
            path = self.log_path

            f_lst = os.listdir(path)
            f_lst.sort(reverse=True)

            f_name = f_lst[0]

            # 최신 날짜 로그 파일 1개
            return '/'.join( [path, f_name] )

        return 1

    def get_std_uptime(self) -> Pandas_DataFrame:

        f_lst = self.get_fileNames(sys._getframe(0).f_code.co_name)

        sum_df = pd.DataFrame()

        for f_name in f_lst:
            tmp_df = pd.read_csv(f_name, parse_dates=['datetime'])
            tmp_df.columns = ['datetime', 'loadAverage_1', 'loadAverage_2', 'loadAverage_3']
            sum_df = sum_df.append(tmp_df, ignore_index=True)
        else:
            sum_df.columns = ['datetime', 'loadAverage_1', 'loadAverage_2', 'loadAverage_3']
            sum_df.drop_duplicates(subset='datetime', keep='first', inplace=True)

        return sum_df

    def get_today_uptime(self)-> Pandas_DataFrame:

        f_name = self.get_fileNames(sys._getframe(0).f_code.co_name)

        date = re.search(self.fileDate, f_name).group()
        date = datetime.strptime(date, '%Y-%m-%d')

        datetimes: Pandas_Series = pd.Series(dtype='datetime64[ns]')
        loadAverages: Pandas_Series = pd.Series(dtype=float)

        with open(f_name, 'r') as f:

            while True:

                line = f.readline()

                if not line : break

                time = re.search(self.uptimeTime, line).group()
                time = datetime.strptime(time, '%H:%M:%S')

                dt = date + timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)
                datetimes = datetimes.append(pd.Series(dt), ignore_index=True)

                loadAverage = self.loadAverage.findall(line)[0]
                loadAverages = loadAverages.append(pd.Series(loadAverage), ignore_index=True)

            merge_df = pd.concat([datetimes, loadAverages], axis=1)
            merge_df.columns = ['datetime', 'loadAverage']

            dictionary = { ',': '', '\n': '' }

            for key in dictionary.keys():
                merge_df['loadAverage'] = merge_df['loadAverage'].str.replace(key, dictionary[key])
            else:
                merge_df['loadAverage_1'] = merge_df['loadAverage'].str.split(' ').str[-3]
                merge_df['loadAverage_2'] = merge_df['loadAverage'].str.split(' ').str[-2]
                merge_df['loadAverage_3'] = merge_df['loadAverage'].str.split(' ').str[-1]

                merge_df.drop('loadAverage', axis=1, inplace=True)
                merge_df[['loadAverage_1', 'loadAverage_2', 'loadAverage_3']] = merge_df[['loadAverage_1', 'loadAverage_2', 'loadAverage_3']].astype(float)

                merge_df.drop_duplicates(subset='datetime', keep='first', inplace=True)

        return merge_df


class MpstatPreprocessor(PreprocessorBase, RegexMixin):

    def __init__(
        self,
        mpstat_log_path: str,
        mpstat_prep_path: str,
        days_ago: int | None = None
    ):
        PreprocessorBase.__init__(self, log_path=mpstat_log_path, prep_path=mpstat_prep_path, days_ago=days_ago)
        RegexMixin.__init__(self)


    def get_fileNames(self, method_name) -> List[str] | str:
        """
        호출한 함수의 이름을 읽어, 함수의 이름 별로 읽어와야 할 파일들의 목록을 list로 반환한다.

        Parameters
        ----------
        method_name : str
           호출한 함수의 이름

        Returns
        -------
        list or str
           해당 함수를 호출한 함수의 이름에 따라, 전처리할 파일의 절대경로를 반환한다.
           
           만약, 호출 함수가 get_std_mpstat라면 파일들의 list를 반환하고
           호출 함수가 get_today_mpstat라면 파일 한 개의 절대경로를 str로 반환한다.

        See Also
        --------
        self.get_std_uptime.__name__ : 실행날짜 기준 지난 N일(당일 제외)의 토크나이징된 csv 파일을 불러와 결합한 pandas.core.frame.DataFrame을 제공하는 함수
        self.get_today_uptime.__name__ : 실행날짜 기준 당일의 log파일을 토크나이징한 pandas.core.frame.DataFrame을 제공하는 함수
        """               

        if method_name == self.get_std_mpstat.__name__:

            datatype = 'csv'
            path = self.prep_path

            f_lst = os.listdir(path)
            f_lst.sort(reverse=True)

            f_lst = f_lst[0:self.days_ago]
            f_lst = [ '/'.join( [path, f_name] ) for f_name in f_lst if datatype in f_name ]
            f_lst.sort()

            return f_lst

        elif method_name == self.get_today_mpstat.__name__:

            datatype = 'log'    
            path = self.log_path

            f_lst = os.listdir(path)
            f_lst.sort(reverse=True)

            f_name = f_lst[0]

            # 최신 날짜 로그 파일 1개
            return '/'.join( [path, f_name] )

        return 1


    def get_std_mpstat(self) -> Pandas_DataFrame:

        f_lst = self.get_fileNames(sys._getframe(0).f_code.co_name)

        sum_df = pd.DataFrame()

        for f_name in f_lst:
            tmp_df = pd.read_csv(f_name, parse_dates=['datetime'])
            sum_df = sum_df.append(tmp_df, ignore_index=True)
        else:
            sum_df.drop_duplicates(subset='datetime', keep='first', inplace=True)
            sum_df.columns = ['datetime', 'cpu_usage']

        return sum_df


    def get_today_mpstat(self) -> Pandas_DataFrame:

        # 최신 날짜 로그 파일 N개 추출
        f_name = self.get_fileNames(sys._getframe(0).f_code.co_name)

        date = re.search(self.fileDate, f_name).group()
        date = datetime.strptime(date, '%Y-%m-%d')

        datetimes: Pandas_Series = pd.Series(dtype='datetime64[ns]')
        cpu_usages: Pandas_Series = pd.Series(dtype=float)

        with open(f_name, 'r') as f:

            while True:

                line = f.readline()

                if not line : break

                time = re.search(self.mpstatTime, line).group()
                time = datetime.strptime(time, '%I:%M:%S %p')

                dt = date + timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)
                datetimes = datetimes.append(pd.Series(dt), ignore_index=True)

                cpu_usage = 100 - float(self.idle.findall(line)[0].replace('\n', ''))
                cpu_usages = cpu_usages.append(pd.Series(cpu_usage), ignore_index=True)

            merge_df = pd.concat([datetimes, cpu_usages], axis=1)
            merge_df.columns = ['datetime', 'cpu_usage']

            merge_df.drop_duplicates(subset='datetime', keep='first', inplace=True)

        return merge_df