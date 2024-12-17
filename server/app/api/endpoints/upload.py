from fastapi import APIRouter, File, UploadFile, HTTPException
from typing import List
import pandas as pd
import io
from app.database import db
from app.utils.table_mappings import TABLE_STRUCTURE_MAP
from fastapi.encoders import jsonable_encoder

router = APIRouter()

@router.post("/upload")
async def upload_csv(files: List[UploadFile] = File(...)):
    response_details = []  # Lista para armazenar detalhes da resposta

    for file in files:
        try:
            # Lê o CSV em um DataFrame
            content = await file.read()
            df = pd.read_csv(io.BytesIO(content))

            # Converte os nomes das colunas para snake_case
            df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

            # Detectar tabela com base na estrutura
            detected_table = None
            for table_name, expected_columns in TABLE_STRUCTURE_MAP.items():
                if set(expected_columns).issubset(set(df.columns)):
                    detected_table = table_name
                    break

            if not detected_table:
                raise HTTPException(
                    status_code=400,
                    detail=f"A estrutura do arquivo {file.filename} não corresponde a nenhuma tabela conhecida."
                )

            # Verificar se a tabela já existe
            with db.get_cursor() as cursor:
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                    (detected_table,)
                )
                table_exists = cursor.fetchone() is not None

                if table_exists:
                    # Obter dados existentes no banco para comparar
                    existing_data = pd.read_sql_query(f"SELECT * FROM {detected_table}", db.connection)

                    # Verificar duplicatas
                    new_data = pd.concat([df, existing_data]).drop_duplicates(keep=False)

                    if not new_data.empty:
                        # Incrementar apenas dados únicos
                        new_data.to_sql(detected_table, db.connection, if_exists="append", index=False)

                    # Obter contagem final de registros
                    final_count = int(pd.read_sql_query(f"SELECT COUNT(*) AS total FROM {detected_table}", db.connection).iloc[0, 0])
                    response_details.append({
                        "table": detected_table,
                        "message": f"{len(new_data)} registro(s) foram adicionados.",
                        "total_records": final_count
                    })

                else:
                    # Criar a tabela e adicionar os dados
                    df.to_sql(detected_table, db.connection, if_exists="replace", index=False)

                    # Obter contagem de registros após criação
                    total_count = int(pd.read_sql_query(f"SELECT COUNT(*) AS total FROM {detected_table}", db.connection).iloc[0, 0])
                    response_details.append({
                        "table": detected_table,
                        "message": "A tabela foi criada e os dados foram carregados.",
                        "total_records": total_count
                    })

        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Erro ao processar {file.filename}: {str(e)}"
            )

    # Converter todos os valores de retorno para tipos nativos
    return jsonable_encoder({
        "message": "Processamento concluído.",
        "details": response_details
    })