import os
import oracledb
import pandas as pd

def oracle2pd(db, q):

    if db == 'ecap':
    
        try:
        
            conexion = oracledb.connect(
                user=os.environ.get('ECAP_USERNAME'),                
                password=os.environ.get('ECAP_PASSWORD'),          
                dsn=os.environ.get('ECAP_DSN')
            )
        
        except oracledb.DatabaseError as e:
        
            print(f"Connection error: {e}")
            
            return None

    elif db == 'cronicitat':

        try:
        
            conexion = oracledb.connect(
                user=os.environ.get('CRON_USERNAME'),                
                password=os.environ.get('CRON_PASSWORD'),          
                dsn=os.environ.get('CRON_DSN')
            )
        
        except oracledb.DatabaseError as e:
        
            print(f"Connection error: {e}")
            
            return None

    else:

        try:
        
            conexion = oracledb.connect(
                user=os.environ.get('EXADOT_USERNAME'),                
                password=os.environ.get('EXADOT_PASSWORD'),          
                dsn=os.environ.get('EXADOT_DSN')
            )
        
        except oracledb.DatabaseError as e:
        
            print(f"Connection error: {e}")
            
            return None
        
    try:
        
        cursor = conexion.cursor()
        cursor.execute(q)
        
        chunks = []
        
        while True:
            chunk = cursor.fetchmany(1000)
            if not chunk:
                break
            chunk_df = pd.DataFrame(chunk, columns=[col[0] for col in cursor.description])
            chunks.append(chunk_df)
        
        data = pd.concat(chunks, ignore_index=True)

        cursor.close()
        conexion.close()

        return data
    
    except Exception as e:
        
        print(f"Query error: {e}")
        
        return None