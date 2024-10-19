import os
import warnings
import pyodbc
import pandas as pd

warnings.filterwarnings('ignore', category=UserWarning)

def ccc2pd(q):

    conn_str = (
    f"DRIVER={{FreeTDS}};"
    f"SERVER={os.getenv('SYBASE_HOST_JX_UCI_DW')};"
    f"PORT={os.getenv('SYBASE_PORT')};"
    f"DATABASE={os.getenv('SYBASE_DATABASE')};"
    f"UID={os.getenv('SYBASE_USER_IT')};"
    f"PWD={os.getenv('SYBASE_PASSWORD_IT')};"
    f"TDS_Version={os.getenv('SYBASE_TDS_VERSION')};"
    )

    try:
        
        conexion = pyodbc.connect(conn_str)
        
    except pyodbc.Error as e:
        
        print(f"Connection error: {e}")
        
        return None
    
    try:

        chunks = []
        for chunk in pd.read_sql_query(sql=q, con=conexion, chunksize=1000):
            chunks.append(chunk)
        data = pd.concat(chunks, ignore_index=True)

        # cursor.close()
        conexion.close()

        return data
    
    except Exception as e:
        
        print(f"Query error: {e}")
        
        return None