from fastapi import APIRouter, HTTPException
from app.database import db

router = APIRouter()

@router.get("/tables")
async def list_tables():
    try:
        with db.get_cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]  # Recupera nomes das tabelas
            return {"tables": tables}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao listar tabelas: {str(e)}"
        )
