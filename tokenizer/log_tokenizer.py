#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : log_tokenizer.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.11.11
#    파라미터      : None
#    설명          : 
######################################################

import warnings
warnings.filterwarnings(action='ignore') 

import os
import sys

if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 6):
    raise Exception("This program requires at least Python 3.7")


# 기본 파일 경로
# BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = '/lake_etl/etl/mart/anomaly_detection'
sys.path.append(BASE_PATH)

from regex_mixin import RegexMixin

import pandas as pd
import numpy as np

import re

from datetime import datetime, timedelta

from typing import List

exit_code = 0


# 로그 경로 (Linux 쉘 명령어로 저장된 text 파일 로그)
UPTIME_LOG_PATH = f'{BASE_PATH}/uptime_log'
MPSTAT_LOG_PATH = f'{BASE_PATH}/mpstat_log'

# 토크나이징 후 csv를 저장할 파일 경로 (없으면 생성)
UPTIME_SAVE_PATH = f'{BASE_PATH}/uptime_prep'
MPSTAT_SAVE_PATH = f'{BASE_PATH}/mpstat_prep'


__all__ = ['LogTokenizer']

class LogTokenizer(RegexMixin):

    def __call__(self, days_ago=2):
        """
        
        Parameters
        ----------        

        >>> tokenizer = LogTokenizer()
        >>> tokenizer(days_ago=3)
        """

        if isinstance(days_ago, int):
            return setattr(LogTokenizer, '__days_ago', days_ago)
        else:
            raise TypeError("days_ago must be an integer")

        return self

    def _get_fileNames(self, LOG_PATH: str, days_ago: int) -> List[str]:
        file_lst = [ f_name for f_name in os.listdir(LOG_PATH) if any(format in f_name for format in ['log', 'csv']) ]
        file_lst = sorted(file_lst, reverse=True)[1:days_ago]

        return file_lst        

    def preprocess_uptime_log(self):
        """
        
        >>> tokenizer = LogTokenizer()
        >>> tokenizer.preprocess_uptime_log()

        """
        # 최신 날짜 로그 파일 리스트
        file_lst = self._get_fileNames(UPTIME_LOG_PATH, getattr(LogTokenizer, '__days_ago', 2))
        
        for f_name in file_lst:

            date = re.search(self.fileDate, f_name).group()
            date = datetime.strptime(date, '%Y-%m-%d')

            datetimes: Pandas_Series = pd.Series(dtype='datetime64[ns]')
            loadAverages: Pandas_Series = pd.Series(dtype=float)

            with open(f'{UPTIME_LOG_PATH}/{f_name}', 'r') as f:

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

                cond1 = merge_df.datetime < merge_df.datetime.quantile(0.01)
                cond2 = merge_df.index.isin(merge_df.index[-5:])
                idxs = merge_df[cond1 & cond2].index
                
                merge_df.loc[idxs, 'datetime'] =  merge_df.loc[idxs, 'datetime'] + timedelta(days=1)

                dictionary = { ',': '', '\n': '' }

                for key in dictionary.keys():
                    merge_df['loadAverage'] = merge_df['loadAverage'].str.replace(key, dictionary[key])
                else:
                    merge_df['loadAverage_1'] = merge_df['loadAverage'].str.split(' ').str[-3]
                    merge_df['loadAverage_2'] = merge_df['loadAverage'].str.split(' ').str[-2]
                    merge_df['loadAverage_3'] = merge_df['loadAverage'].str.split(' ').str[-1]
                    
                    merge_df.drop('loadAverage', axis=1, inplace=True)
                
                # daily 파일 저장
                merge_df.to_csv(f"{UPTIME_SAVE_PATH}/uptime_{datetime.strftime(date, '%Y-%m-%d')}_pre.csv", index=False) 
                    

    def preprocess_mpstat_log(self):
        """

        >>> LogTokenizer().preprocess_mpstat_log()

        """

        # 최신 날짜 로그 파일 리스트
        file_lst = self._get_fileNames(MPSTAT_LOG_PATH, getattr(LogTokenizer, '__days_ago', 2))

        for f_name in file_lst:

            date = re.search(self.fileDate, f_name).group()
            date = datetime.strptime(date, '%Y-%m-%d')

            datetimes: Pandas_Series = pd.Series(dtype='datetime64[ns]')
            cpu_usages: Pandas_Series = pd.Series(dtype=float)        

            with open(f'{MPSTAT_LOG_PATH}/{f_name}', 'r') as f:

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

                cond1 = merge_df.datetime < merge_df.datetime.quantile(0.01)
                cond2 = merge_df.index.isin(merge_df.index[-5:])
                idxs = merge_df[cond1 & cond2].index

                merge_df.loc[idxs, 'datetime'] =  merge_df.loc[idxs, 'datetime'] + timedelta(days=1)

                # daily 파일 저장
                merge_df.to_csv(f"{MPSTAT_SAVE_PATH}/mpstat_{datetime.strftime(date, '%Y-%m-%d')}_pre.csv", index=False)