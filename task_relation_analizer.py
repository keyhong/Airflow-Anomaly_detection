import os
from typing import List, Tuple

import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings(action='ignore')

from collections import Counter

__all__ = ["TaskRelationAnalizer"]

class TaskRelationAnalizer:

    # __dag_abs_path = f"{os.environ['AIRFLOW_HOME']}/dags"
    __dag_abs_path = '/home/user/b2en/dev/anomaly_detection/airflow_dags'

    def __init__(
        self,
        dag_id: str=None,
        task_id: str=None # context['__task_id']를 텍스트 처리한 task 이름
    ):
        self.__dag_id = dag_id
        self.__task_id = task_id
        # self.dag_relation_df = self.comment_preprocess()

    @property
    def task_id(self):
        return self.__task_id

    @task_id.setter
    def task_id(self, task_id):
        self.__task_id = task_id

    @classmethod
    def get_all_py_dag_paths(cls):
        """$AIRFLOW_HOME/dags 안에 있는 모든 파일의 절대 경로를 반환하는 함수"""

        files = os.listdir(cls.__dag_abs_path)
        
        all_dags = []
        
        # 제외할 task (STEP_9_ANOMALY_1_PREP : 이상탐지 프로그램) 제거
        except_dag_lst = [ 'STEP_9_ANOMALY_1_PREP', 'STEP_9_2_TEST' ]
        
        # 제외된 task가 아니면서 & 파일 형식이 .py인 파일만 추출
        for python_dag in files:
            
            if ( os.path.splitext(python_dag)[0] not in except_dag_lst ) & ( os.path.splitext(python_dag)[-1] == '.py' ):
                all_dags.append(cls.__dag_abs_path + '/' + python_dag)
        else:
            all_dags.sort()
        
        return all_dags

    def analyze_relation(self, dags_paths: List[str]):

        # output DataFrame
        all_df = pd.DataFrame(columns=['dag_id', 'task_id', 'trigger'])

        for dag_path in dags_paths:

            ''' 로그 파일 열기 '''
            # open dag log file
            f = open(dag_path, 'r')
            self.lines = f.readlines()

            self.lines = self.comment_preprocess(self.lines)

            ''' task 연결관계를 분석하여 데이터 프레임을 생성하기 '''
            # task 순서가 있는 라인들만 추출
            task_lines = [ line.replace('\n', '>>').replace(' ', '') for line in self.lines if line.find('>>') > 0 ]
            
            letter = ''

            for line in task_lines:
                letter += line
            else:
                letter = letter.split('>>')

            del_lst = []

            for i in range(1, len(letter)):
                if letter[i-1] == letter[i]:
                    del_lst.append(letter[i-1])
            else:
                letter = letter[:-1]
                
                if del_lst:
                    
                    # 반복되지 않지만, 첫 group_task를 제거 하는 과정
                    if del_lst[0][:-1] + str(int(del_lst[0][-1])-1) in letter:
                        letter.remove(del_lst[0][:-1] + str(int(del_lst[0][-1])-1))

                    # 반복되는 task(group task)를 제거하는 과정
                    del_item_dict = Counter(del_lst)
                    
                    for key, value in del_item_dict.items():
                        for _ in range(2):
                            letter.remove(key)

            ######################################################################################################                    

            next_dag_nm, start_nm, end_nm = None, None, None
            trigger_lst, next_dag_lst = [], []
            
            for i in range(len(letter)):

                if (letter[i].startswith('[')) & (letter[i].endswith(']')):
 
                    letter[i] = letter[i].lstrip('[').rstrip(']')

                    # list내 task를 분리
                    if ',' in letter[i]:
                        letter[i] = letter[i].split(',')
                    
                if isinstance(letter[i], str):
                    
                    # START 더미 오퍼레이터 판별
                    if letter[i].endswith('START') > 0:
                        # dag_name = letter[i][0:letter[i].find('START')-1]
                        start_nm = letter[i]

                    # END 더미 오퍼레이터 판별
                    if letter[i].endswith('END') > 0:
                        end_nm = letter[i]
                    
                    # TRIGGER 판별
                    if letter[i].startswith('TRIGGER') > 0:
                        
                        next_dag_nm = letter[i][letter[i].find('TRIGGER')+len('TRIGGER')+1:]
                        next_dag_lst.append(next_dag_nm)
                        
                        trigger_lst.append(letter[i])
            else:
                if start_nm is not None:
                    letter.remove(start_nm)

                if end_nm is not None:
                    letter.remove(end_nm)

            # 임시 데이터프레임을 생성하여 dag_id, task_id, trigger 이름 저장
            tmp_df = pd.DataFrame(columns=['dag_id', 'task_id', 'trigger'])
            
            # task_id
            for trigger in trigger_lst:
                letter.remove(trigger)
            else:
                tmp_df['task_id'] = pd.Series(letter)
                del trigger_lst
            
            # dag_id
            tmp_df['dag_id'] = dag_path.split('/')[-1].split('.')[0]

            # trigger
            if len(next_dag_lst) >= 2:
                tmp_df['trigger'] = [ next_dag_lst.copy() for _ in range(len(tmp_df)) ]
            elif len(next_dag_lst) == 1:
                tmp_df['trigger'] = next_dag_lst[0]
            else:
                tmp_df['trigger'] = np.nan
                
            all_df = all_df.append(tmp_df, ignore_index=True)
            f.close()
            
        self.all_df = all_df
            

    @classmethod
    def comment_preprocess(cls, lines):
        
        ''' 로그 파일에서 주석은 제거하기 '''
        lines = cls._remove_case_1(lines)
        lines = cls._remove_case_2(lines)
        lines = cls._remove_case_3(lines)

        return lines

    @classmethod
    def _remove_case_1(cls, lines):
        ''' 여러줄 주석이 한 줄에 끝나는 경우인 line 제거 '''

        # 여러행 주석 종류가  """ ~ CODE ~ """ 인 경우 (단, 한 줄에 """로 시작해서 """로 끝나는 경우)
        comment_case_1 = [ idx for idx in range(len(lines)) if (lines[idx].startswith('"""')) & (lines[idx].endswith('"""') & (lines[idx].count('"""') > 1)) ]

        # 여러행 주석 종류가  """ ~ CODE ~ """ 인 경우 (단, 한 줄에 '''로 시작해서 '''로 끝나는 경우)
        comment_case_2 = [ idx for idx in range(len(lines)) if (lines[idx].startswith("'''")) & (lines[idx].endswith("'''") & (lines[idx].count("'''") > 1)) ]

        if comment_case_1:
            for i in sorted(comment_case_1, reverse=True):
                del lines[i]

        if comment_case_2:
            for i in sorted(comment_case_2, reverse=True):
                del lines[i]

        return lines


    @classmethod
    def _remove_case_2(cls, lines):
        ''' 여러줄 주석이 여러 줄에 끝나는 경우 line 제거 '''

        from collections import namedtuple
        Comment = namedtuple('Comment', 'idx1, idx2') # 여러행 주석의 라인 번호 정보를 담을 자료구조 생성

        # 여러행 주석 종류가  """ ~ CODE ~ """ 인 경우
        comment_case_3 = [ idx for idx in range(len(lines)) if lines[idx].find('"""') >= 0 ]

        # 여러행 주석 종류가  ''' ~ CODE ~ ''' 인 경우
        comment_case_4 = [ idx for idx in range(len(lines)) if lines[idx].find("'''") >= 0 ] 

        comment_lst = []

        if comment_case_3:
            for i in range(0, len(comment_case_3), 2):
                comment_lst.append(Comment(comment_case_3[i], comment_case_3[i+1]+1))

        if comment_case_4:
            for i in range(0, len(comment_case_4), 2):
                comment_lst.append(Comment(comment_case_4[i], comment_case_4[i+1]+1))
        
        comment_lst = sorted(comment_lst, key=lambda x: x.idx1, reverse=True)

        for i_1, i_2 in comment_lst:
            del lines[i_1 : i_2]
        
        return lines
            

    @classmethod
    def _remove_case_3(cls, lines):
        ''' 한 줄 주석인 경우 line 제거 '''

        comment_case_5 = [ idx for idx in range(len(lines)) if lines[idx].startswith('#') ]
        
        if comment_case_5:
            for i in sorted(comment_case_5, reverse=True):
                del lines[i]

        return lines

    def find_related_err(self, compared_task: str):
        """
        compared_task : 에러 태스크와 상관이 있는 지 확인할 task

        """

        if self.all_df is None:
            raise Error('Class.comment_preprocess() 를 먼저 사용하세요.')

        # 메모리 에러 task의 아래 task ~ 같은 dag의 마지막 task 추출
        for idx in self.all_df.index:

            if compared_task in self.all_df.loc[idx, 'task']:

                start_dag = self.all_df.loc[idx, 'dag']

                first_idx = idx + 1
                last_idx = self.all_df[self.all_df.dag == self.all_df.loc[idx, 'dag']].index.max()
                break
        else:
            err_task_df = self.all_df.loc[first_idx:last_idx]

            
        # 에러난 task의 하위 dag(trigger dag)를 담을 리스트
        err_lst = []
        self._recursive_dag_find(start_dag, err_lst)

        self.all_df[ self.all_df.dag.isin(err_lst) ] = ''

        # 메모리 에러 task의 하위 dag 추출
        if err_lst:
            err_task_df = err_task_df.append( self.all_df[self.all_df.dag.isin(err_lst)] )

        result = []

        for task in a.all_df.task:
            result.extend(list(task))

    def _recursive_dag_find(self, start_dag: str, err_lst: List) -> List:
        """
        ** Call-By-Reference
        """
        import logging

        try:
            if self.all_df[self.all_df.dag == start_dag].empty:
                return
            elif self.all_df[self.all_df.dag == start_dag].iloc[0, 2] is np.nan:
                return
        except NameError:
            logging.info('NameError : unique_df is not exist.')
        
        next_trigger = self.all_df[self.all_df.dag == start_dag].iloc[0, 2]

        if isinstance(next_trigger, list):
            err_lst.extend(next_trigger)
            
            for next_dag in next_trigger:
                _recursive_dag_find(next_dag, err_lst)
        
        if isinstance(next_trigger, str):
            err_lst.append(next_trigger)
            _recursive_dag_find(next_trigger, err_lst)