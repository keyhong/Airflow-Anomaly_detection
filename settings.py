#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : settings.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.11.11
#    파라미터      : None
#    설명          : 
######################################################

# 환경세팅 기본 폴더 절대경로
AIRFLOW_HOME: str = os.environ.get('AIRFLOW_HOME', '~/airflow') # 사용자 환경변수에서 AIRFLOW_HOME 탐색
ETL_HOME: str = cfg_reader.get_value('BASE_DIR', 'ETL_HOME')


# 로그 폴더 절대경로
AIRFLOW_TASK_LOG_DIR: str = cfg_reader.get_value('LOG_DIR', 'AIRFLOW_TASK_LOG_DIR')

UPTIME_LOG_DIR: str = cfg_reader.get_value('LOG_DIR', 'UPTIME_LOG_DIR')
MPSTAT_LOG_DIR: str = cfg_reader.get_value('LOG_DIR', 'MPSTAT_LOG_DIR')

DW_ERROR_LOG_DIR: cfg_reader.get_value('LOG_DIR', 'DW_ERROR_LOG_DIR')
DM_ERROR_LOG_DIR: cfg_reader.get_value('LOG_DIR', 'DM_ERROR_LOG_DIR')


# 로그 전처리 폴더 절대경로
UPTIME_PREP_PATH: str = cfg_reader.get_value('LOG_PREP_DIR', 'UPTIME_PREP_DIR')
MPSTAT_PREP_PATH: str = cfg_reader.get_value('LOG_PREP_DIR', 'MPSTAT_PREP_DIR')