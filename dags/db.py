import oracledb


def get_connection():
    """
    Returns an oracledb connection.
    Reads from Airflow Variables when available, otherwise falls back
    to hardcoded local-dev defaults. Call inside task functions only —
    never at module import time.
    """
    try:
        from airflow.models import Variable
        user     = Variable.get("ORACLE_USER",     default_var="retail_dwh")
        password = Variable.get("ORACLE_PASSWORD", default_var="secret")
        dsn      = Variable.get("ORACLE_DSN",      default_var="localhost:1521/ORCLPDB1")
    except Exception:
        user     = "retail_dwh"
        password = "secret"
        dsn      = "localhost:1521/ORCLPDB1"

    return oracledb.connect(user=user, password=password, dsn=dsn)


def get_csv_path(default_relative: str) -> str:
    """
    Returns the CSV path from an Airflow Variable if set,
    otherwise returns the given default (absolute) path.
    """
    try:
        from airflow.models import Variable
        return Variable.get("RETAIL_CSV_PATH", default_var=default_relative)
    except Exception:
        return default_relative
