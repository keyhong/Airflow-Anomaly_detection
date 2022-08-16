

# setting


 os.path.expanduser(conf.get_mandatory_value('core', 'DAGS_FOLDER'))


AIRFLOW_LOG_FOLDER: str = ''
UPTIME_LOG_FOLDER: str = ''
MPSTAT_LOG_FOLDER: str = ''
UPTIME_PREP_PATH: str = ''
MPSTAT_PREP_PATH: str = ''

MPSTAT_PREP_FOLDER

def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def get_airflow_home():
    return expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))

AIRFLOW_HOME = get_airflow_home()


os.environ.get('ETL_HOME', '~/airflow')

f"{os.environ['ETL_HOME']}/mapr/mapr.daegu.go.kr/ETL/lake_etl/airflow/logs"


conf = 