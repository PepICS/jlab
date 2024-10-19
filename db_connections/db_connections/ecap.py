import os
import oracledb
import pandas as pd

def ecap2pd(q):
    
    try:
        
        conexion = oracledb.connect(
            user=os.environ.get('ECAP_USERNAME'),                
            password=os.environ.get('ECAP_PASSWORD'),          
            dsn=os.environ.get('ECAP_DSN')
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