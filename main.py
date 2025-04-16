from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from typing import List, Dict, Any
import pandas as pd
from Backend import get_connection
from snowflake.connector.errors import ProgrammingError

app = FastAPI(title="Snowflake Record Search API")

class SearchRequest(BaseModel):
    email: EmailStr
    phone: str

class TableData(BaseModel):
    table_name: str
    rows: List[Dict[str, Any]]

@app.post("/search", response_model=List[TableData])
def search_records(payload: SearchRequest):
    email = payload.email
    phone = payload.phone
    results = []

    try:
        conn = get_connection()
        cur = conn.cursor()
        tables = ['Table1', 'Table2', 'Table3']

        for table in tables:
            query = f"""
                SELECT * FROM {table}
                WHERE Email = %s AND MobilePhone = %s
            """
            cur.execute(query, (email, phone))
            rows = cur.fetchall()
            if rows:
                colnames = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                results.append({
                    "table_name": table,
                    "rows": df.to_dict(orient="records")
                })

        cur.close()
        conn.close()

        if not results:
            raise HTTPException(status_code=404, detail="No matching records found in any table.")
        
        return results

    except ProgrammingError as e:
        raise HTTPException(status_code=400, detail=f"Snowflake error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
