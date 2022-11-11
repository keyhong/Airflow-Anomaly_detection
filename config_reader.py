#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : config_reader.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.11.11
#    파라미터      : None
#    설명          : config 파일을 파싱하여 가져오는 파일 (default : 해당 파일과 동일 경로에 있는 *.cfg를 읽는다)
######################################################

import os
import glob
import configparser
from typing import List

from detection_logger import logger

class ConfigReader:

    def __new__(cls, CFG_DIR: str=None):

        # config 위치 경로에 대한 인풋값 설정이 없다면, 현재 파일 실행 디렉토리에 config 파일이 있다고 가정
        if CFG_DIR is None: 
            cls.CFG_DIR = os.path.dirname(os.path.abspath(__file__))         
        # class의 input 파라미터에 대한 타입 검정        
        elif isinstance(CFG_DIR, str):
            raise TypeError('input var CFG_DIR should be str.')
        # config 위치 경로에 대한 인풋값 설정이 잘못됬다면(폴더가 없는 경우)에도 현재 파일 실행 디렉토리로 config 파일 위치 폴더로 지정
        elif not isdir(CFG_DIR): 
            logger.error(f'input variable CFG_DIR "{CFG_DIR}"  is not exists. Set base CFG_DIR "{os.path.dirname(os.path.abspath(__file__))}"')
            cls.CFG_DIR = os.path.dirname(os.path.abspath(__file__)) 

        # glob 라이브러리를 이용해 폴더에 있는 .cfg 파일의 절대경로를 파악. cfg 파일은 하나만 있어야 하므로, assert를 통해 값을 검정
        cfg_file: List[str] = glob.glob(f'{os.path.join(cls.CFG_DIR, "*.ini")}')
        assert len(cfg_file) == 1, f'*.ini file should be only one in CFG_DIR "{cls.CFG_DIR}"'

        # 정상 실행시, configparser 라이브리의 ConfigParser 인스턴스를 만들어 파일 읽기
        cls.cfg_reader = configparser.ConfigParser()
        cls.cfg_reader.read(cfg_file)

        obj = super().__new__(cls)
        return cls

    @classmethod
    def get_value(cls, section:str, option: str) -> str:

        try:
            return_value = cls.cfg_reader.get(section, option)

        # Section이 없는 경우
        except configparser.NoSectionError as err: 
            logger.error(f'Input Parameter section "{section}" is not exist.')
            raise err

        # option 없는 경우
        except configparser.NoOptionError as err:
            logger.error(f'Input Parameter Section "{option}" is not exist.')
            raise err
        
        return return_value


def __test__():

    logger.info(f'{os.path.basename(__file__)} 테스트 시작\n')

    # cfg파일 중 일부 추출
    '''
    [FOLDER_PATH]
    ETL_HOME = '/mapr/mapr.daegu.go.kr/ETL'
    '''

    # reader 인스턴스 생성
    cfg_reader = ConfigReader()

    # 정상 데이터 기재 체큰
    section_name = 'FOLDER_PATH'
    logger.info(f'section : "{section_name}"을 확인합니다.')

    option_name = 'ETL_HOME'
    logger.info(f'request_option : "{option_name}"을 확인합니다.')

    response = cfg_reader.get_value(section_name, option_name)
    logger.info(f'respose : "{response}"을 정상적으로 받아 왔습니다.\n')


    # 에러 데이터 기재 예외처리(section이 없는 경우)
    try:
        section_name = 'NO_Section'
        option_name = 'ETL_HOME'
        response = cfg_reader.get_value(section_name, option_name)
    except configparser.NoSectionError: 
        logger.error(f'section : "{section_name}"는 없습니다.\n')


    # 에러 데이터 기재 예외처리(section이 없는 경우)
    try:
        section_name = 'CHECK_OPTION'
        option_name = 'NO_Option'
    except configparser.NoOptionErrorr:
        logger.error(f'option : "{option_name}"는 없습니다.\n')

    logger.info(f'{os.path.basename(__file__)} 테스트 시작\n')


if __name__ == '__main__':
    __test__()
   