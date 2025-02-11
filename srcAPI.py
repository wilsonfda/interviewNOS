from fastapi import FastAPI, HTTPException
import sqlite3
from pydantic import BaseModel
from typing import List
import uvicorn

app = FastAPI()

db_path = "data/cp7_data.db"

class CodigoPostal(BaseModel):
    CP7: str
    Concelho: str
    Distrito: str

def get_db_connection():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/codigos_postais", response_model=List[CodigoPostal])
def get_codigos_postais():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CP7, Concelho, Distrito FROM cp7_results")
    results = cursor.fetchall()
    conn.close()

    if not results:
        raise HTTPException(status_code=404, detail="Nenhum código postal encontrado")

    return [dict(row) for row in results]

@app.get("/codigos_postais/{codigo_postal}", response_model=CodigoPostal)
def get_codigo_postal(codigo_postal: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CP7, Concelho, Distrito FROM cp7_results WHERE CP7 = ?", (codigo_postal,))
    result = cursor.fetchone()
    conn.close()

    if result is None:
        raise HTTPException(status_code=404, detail="Código postal não encontrado")

    return dict(result)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)