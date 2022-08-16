    ######################################################
#    프로그램명    : preprocessor.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.06.06
#    파라미터      :
#    설명          :
#    변경일자      :
#    변경내역      :
######################################################

# from __future__ import annotations

# import os
import sys
import warnings
warnings.filterwarnings(action='ignore')

import pandas as pd
import numpy as np

import re
from datetime import datetime

from typing import List

import argparse

from typing import TypeVar
Pandas_DataFrame = TypeVar('pandas.core.frame.DataFrame')
Pandas_Series = TypeVar('pandas.core.series.Series')

from preprocessor import *

if (sys.version_info[0] == 3) and (sys.version_info[1] < 6):
    raise Exception("This program requires at least Python 3.7")

from threading import RLock

__all__ = [ 'AnomalyDetectionContext' ]


class AnomalyDetectionContext(obejct):

    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create L{RDD} and
    broadcast variables on that cluster.

    .. note:: :class:`SparkContext` instance is not supported to share across multiple
        processes out of the box, and PySpark does not guarantee multi-processing execution.
        Use threads instead for concurrent processing purpose.
    """

    _lock = RLock()

    def __init__(
        self,
        environmet=None,
        db_connector=None,
    ):
        
        self.db_connector = db_connector


    def __repr__(self):
        return "<SparkContext master={master} appName={appName}>".format(
            master=self.master,
            appName=self.appName,
        )

    # create a signal handler which would be invoked on receiving SIGINT
    def signal_handler(signum, frame):
        print(signum)

    # see http://stackoverflow.com/questions/23206787/
    if isinstance(threading.current_thread(), threading._MainThread):
        signal.signal(signal.SIGINT, signal_handler)


'''
    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None, conf=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = gateway or launch_gateway(conf)
                SparkContext._jvm = SparkContext._gateway.jvm

            if instance:
                if (SparkContext._active_spark_context and
                        SparkContext._active_spark_context != instance):
                    currentMaster = SparkContext._active_spark_context.master
                    currentAppName = SparkContext._active_spark_context.appName
                    callsite = SparkContext._active_spark_context._callsite

                    # Raise error if there is already a running Spark context
                    raise ValueError(
                        "Cannot run multiple SparkContexts at once; "
                        "existing SparkContext(app=%s, master=%s)"
                        " created by %s at %s:%s "
                        % (currentAppName, currentMaster,
                            callsite.function, callsite.file, callsite.linenum))
                else:
                    SparkContext._active_spark_context = instance
'''                    


'''
def find_err_task(airflow_past: Pandas_DataFrame, airflow_today: Pandas_DataFrame) -> Pandas_Series:

    tmp_past = pd.DataFrame()
    tmp_past['year'] = airflow_past.datetime.dt.year
    tmp_past['month'] = airflow_past.datetime.dt.month
    tmp_past['day'] = airflow_past.datetime.dt.day
    tmp_past['task'] = airflow_past.task
    tmp_past['idx'] = airflow_past.idx

    # 조건 1 : 정상 범위(지난 N일의 프로그램 동작시간의 최소값 * 0.75 산정)보다 일찍 끝나는 경우
    tmp_past = tmp_past.groupby(by=['year', 'month', 'day', 'task'], as_index=False).max()[['task', 'idx']].groupby(by=['task'], as_index=False).min()
    tmp_past['idx'] = (tmp_past['idx'] * 0.75).astype(int)
    tmp_past.rename(columns={'idx': 'com_idx'}, inplace=True)

    tmp_today = airflow_today.groupby(by='task', as_index=False).max()
    tmp_merge = tmp_today.merge(tmp_past, how='left', on='task')

    err_task = tmp_merge[tmp_merge.idx < tmp_merge.com_idx].task

    return err_task
'''

    def main():
        


    

