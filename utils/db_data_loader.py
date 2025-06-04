import psycopg2
import pandas as pd
import warnings
from utils.constants import CONNECTION_STRING

class DBDataLoader:
    def __init__(self):
        self.conn = psycopg2.connect(CONNECTION_STRING)

    def load_employees(self):
        query = "SELECT * FROM employees"
        with warnings.catch_warnings():
            # Ignoring UserWarnings that may arise from pandas read_sql_query
            # Pandas.read_sql_query prefers a SQLAlchemy engine or connection, not a
            # psycopg2 connection in the latest versions, hence the warning.
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_sql_query(query, self.conn)
        self.close()
        return df

    def load_departments(self):
        query = "SELECT * FROM departments"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_sql_query(query, self.conn)
        self.close()
        return df

    def close(self):
        self.conn.close()