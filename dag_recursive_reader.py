import logging


def recursive_dag_find(start_dag: str, err_lst: list):
    """
    
    ** Call-By-Reference
    
    """

    try:
        if unique_df[unique_df.dag == start_dag].empty:
            return
        elif unique_df[unique_df.dag == start_dag].iloc[0, 2] is np.nan:
            return
    except NameError:
        logging.info('NameError : unique_df is not exist.')
    
    next_trigger = unique_df[unique_df.dag == start_dag].iloc[0, 2]

    if isinstance(next_trigger, list):
        err_lst.extend(next_trigger)
        
        for next_dag in next_trigger:
            recursive_dag_find(next_dag, err_lst)
    
    if isinstance(next_trigger, str):
        err_lst.append(next_trigger)
        recursive_dag_find(next_trigger, err_lst)


# Memory Error를 제외한 다른 에러
check_syntax = set(err_task).difference(set(memory_err_df.task))

unique_df = tot_df.drop_duplicates(subset='dag')

# 메모리 에러 array를 반복문을 돌려, 하위 프로세스의 에러를 포착
for err_task in memory_err_df.task:

    # 메모리 에러 task의 아래 task ~ 같은 dag의 마지막 task 추출
    for idx in tot_df.index.values:
        if err_task in tot_df.loc[idx, 'task']:

            first_idx = idx+1
            last_idx = tot_df[tot_df.dag == tot_df.loc[idx, 'dag']].index.max()

            start_dag = tot_df.loc[idx, 'dag']
            break
    else:
        err_task_df = tot_df.loc[first_idx:last_idx]

    
    
    # 하위 에러를 담을 리스트
    err_lst = []
    recursive_dag_find(start_dag, err_lst)

    # 메모리 에러 task의 하위 dag 추출
    if err_lst:
        err_task_df = err_task_df.append( tot_df[tot_df.dag.isin(err_lst)] )
        del err_lst
    

    for idx in err_task_df.index.values:
        if isinstance(err_task_df.loc[idx, 'task'], list):
            for task in err_task_df.loc[idx, 'task']:
                memory_err_df = memory_err_df.append({
                    'datetime': '',
                    'dag': err_task_df.loc[idx, 'dag'],
                    'task': task,
                    'idx': '',
                    'loadAverage_1': '',
                    'low_threshold': '',
                    'high_threshold': '',
                    'err_type': 'Dependency_Error'
                }, ignore_index=True)
                
        if isinstance(err_task_df.loc[idx, 'task'], str):
            memory_err_df = memory_err_df.append({
                'datetime': '',
                'dag': err_task_df.loc[idx, 'dag'],
                'task': err_task_df.loc[idx, 'task'],
                'idx': '',
                'loadAverage_1': '',
                'low_threshold': '',
                'high_threshold': '',
                'err_type': 'Dependency_Error'
            }, ignore_index=True)            
