import sqlite3
from fastapi import FastAPI, File, UploadFile, HTTPException
from pydantic import BaseModel
import pandas as pd
from typing import List
import openai
import io
import re
import json
from fastapi.encoders import jsonable_encoder
from fastapi import Query
from fastapi.middleware.cors import CORSMiddleware

# Configuração da chave OpenAI
openai.api_key = "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"

# Instanciação do FastAPI
app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão SQLite (base em memória para a POC)
db_connection = sqlite3.connect(":memory:", check_same_thread=False)

# Modelo para consultas
class QueryRequest(BaseModel):
    question: str  # Modelo para receber perguntas do usuário

# Cria uma tabela no SQLite para cada arquivo CSV carregado
@app.post("/upload")
async def upload_csv(files: List[UploadFile] = File(...)):
    # Definir mapeamento de estruturas conhecidas para tabelas
    # O approach aqui é que o usuário deve carregar os arquivos CSV em ordem e o sistema vai detectar a tabela e adicionar os dados
    # Se a tabela já existir, o sistema vai adicionar apenas os dados que ainda não existem
    table_structure_map = {
        # Mapeamento de tabelas e suas colunas esperadas
        "customers": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        ],
        "order_items": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        ],
        "order_payments": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        ],
        "order_reviews": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        ],
        "orders": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ],
        "products": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"  
        ],
         "sellers": [
             "seller_id",
             "seller_zip_code_prefix",
             "seller_city",
             "seller_state"
         ],
         "product_category_name_translation": [
            "product_category_name",
            "product_category_name_english"     
         ]
    }

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
            for table_name, expected_columns in table_structure_map.items():
                if set(expected_columns).issubset(set(df.columns)):
                    detected_table = table_name
                    break

            if not detected_table:
                raise HTTPException(
                    status_code=400,
                    detail=f"A estrutura do arquivo {file.filename} não corresponde a nenhuma tabela conhecida."
                )

            # Verificar se a tabela já existe
            cursor = db_connection.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (detected_table,)
            )
            table_exists = cursor.fetchone() is not None

            if table_exists:
                # Obter dados existentes no banco para comparar
                existing_data = pd.read_sql_query(f"SELECT * FROM {detected_table}", db_connection)

                # Verificar duplicatas
                new_data = pd.concat([df, existing_data]).drop_duplicates(keep=False)

                if not new_data.empty:
                    # Incrementar apenas dados únicos
                    new_data.to_sql(detected_table, db_connection, if_exists="append", index=False)

                # Obter contagem final de registros
                final_count = int(pd.read_sql_query(f"SELECT COUNT(*) AS total FROM {detected_table}", db_connection).iloc[0, 0])
                response_details.append({
                    "table": detected_table,
                    "message": f"{len(new_data)} registro(s) foram adicionados.",
                    "total_records": final_count
                })

            else:
                # Criar a tabela e adicionar os dados
                df.to_sql(detected_table, db_connection, if_exists="replace", index=False)

                # Obter contagem de registros após criação
                total_count = int(pd.read_sql_query(f"SELECT COUNT(*) AS total FROM {detected_table}", db_connection).iloc[0, 0])
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

# Lista as tabelas no SQLite
@app.get("/tables")
async def list_tables():
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]  # Recupera nomes das tabelas
    return {"tables": tables}

# Detecta relações entre tabelas
def detect_relationships():
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    relationships = {}
    for table in tables:
        columns = pd.read_sql_query(f"PRAGMA table_info({table});", db_connection)["name"].tolist()
        for col in columns:
            if col.endswith("_id") or "id" in col.lower():
                relationships.setdefault(col, []).append(table)  # Mapeia relações
    return relationships

@app.post("/query")
async def query_all_tables(request: QueryRequest):
    try:
        # Detectar tabelas no banco de dados
        cursor = db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        # Prompt inicial para a OpenAI
        prompt = f"""
        Você tem um banco de dados SQLite com as seguintes tabelas:
        {', '.join(tables)}

        O usuário fez a seguinte pergunta:
        "{request.question}"

        Classifique o tipo de resposta que o usuário espera com base na pergunta. 
        Escolha entre:
        1. "row": quando o usuário quer os detalhes de um único registro.
        2. "table": quando o usuário quer listar vários registros.
        3. "stats": quando o usuário quer uma estatística ou número único.
        4. "undefined": quando a pergunta não tem relação com os dados ou não pode ser respondida.

        Além disso, crie a consulta SQL válida para SQLite, se aplicável, que atenda à pergunta. Não inclua explicações no resultado, apenas um JSON no seguinte formato:
        {{
            "type": "row" | "table" | "stats" | "undefined",
            "query": "sua consulta SQL ou null"
        }}
        """

        # Gerar a classificação e a query SQL inicial
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[  # Mensagens para a API OpenAI
                {"role": "system", "content": "Você é um assistente que classifica perguntas e gera consultas SQL válidas para SQLite."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=0
        )

        # Extrair a resposta gerada pela OpenAI
        response_content = response["choices"][0]["message"]["content"].strip()

        # Sanitizar a resposta para garantir que seja um JSON válido
        try:
            result = json.loads(response_content)
        except json.JSONDecodeError as decode_error:
            raise HTTPException(
                status_code=500,
                detail=f"Erro ao processar a resposta da OpenAI: {str(decode_error)}"
            )

        # Recuperar o tipo e a query do JSON
        query_type = result["type"]
        sql_query = result.get("query", None)

        # Tratamento para tipo "undefined"
        if query_type == "undefined":
            return {
                "type": "undefined",
                "message": "Não entendi sua pergunta. Por favor, pergunte algo relacionado aos dados disponíveis."
            }

        # Remover blocos de código SQL (se existirem)
        if sql_query and "```sql" in sql_query:
            sql_query = sql_query.split("```sql")[1].split("```")[0].strip()

        if not sql_query:
            raise HTTPException(
                status_code=400,
                detail="Nenhuma consulta SQL válida foi gerada."
            )

        # Executar a query SQL principal
        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Recuperar os nomes das colunas para estruturar o resultado
        column_names = [description[0] for description in cursor.description]

        # Formatar a resposta com base no tipo
        if query_type == "row":
            if len(result) != 1:
                raise HTTPException(
                    status_code=400,
                    detail="Esperava-se um único registro, mas a query retornou múltiplos resultados."
                )
            
            # Montar o registro principal
            row = dict(zip(column_names, result[0]))

            # Verificar se o pedido inclui produtos relacionados
            if "order_id" in column_names or "id" in column_names:
                order_id = row.get("order_id") or row.get("id")
                
                # Procurar produtos relacionados ao pedido
                product_query = f"""
                SELECT p.*
                FROM order_items oi
                JOIN products p ON oi.product_id = p.id
                WHERE oi.order_id = {order_id}
                """
                cursor.execute(product_query)
                products_result = cursor.fetchall()
                product_columns = [description[0] for description in cursor.description]
                products = [dict(zip(product_columns, prod)) for prod in products_result]
                
                # Adicionar produtos ao registro do pedido
                row["products"] = products

            return {
                "type": "row",
                "row": row
            }

        elif query_type == "table":
            rows = [dict(zip(column_names, row)) for row in result]
            return {
                "type": "table",
                "table": rows
            }

        elif query_type == "stats":
            if len(result) == 1 and len(column_names) == 1:
                # Caso seja um único valor estatístico
                return {
                    "type": "stats",
                    "stats": {column_names[0]: result[0][0]}
                }
            else:
                # Caso sejam múltiplos valores estatísticos
                stats = [dict(zip(column_names, row)) for row in result]
                return {
                    "type": "stats",
                    "stats": stats
                }

    except sqlite3.Error as sql_error:
        raise HTTPException(
            status_code=400,
            detail=f"Erro na execução da consulta SQL: {str(sql_error)}"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar a consulta: {str(e)}")

@app.get("/orders")
async def get_orders(
    page: int = Query(1, ge=1),  # Página atual, valor mínimo 1
    page_size: int = Query(10, ge=1, le=100),  # Número de itens por página (máx. 100)
    order_id: str = Query(None),  # Filtrar por `order_id`
    product_id: str = Query(None),  # Filtrar por `product_id`
    customer_id: str = Query(None)  # Filtrar por `customer_id`
):
    try:
        # Construir query base
        base_query = "SELECT * FROM order_items"
        filters = []

        # Adicionar filtros com base nos parâmetros
        if order_id:
            filters.append(f"order_id = '{order_id}'")
        if product_id:
            filters.append(f"product_id = '{product_id}'")
        if customer_id:
            filters.append(f"customer_id = '{customer_id}'")

        # Concatenar os filtros na query base
        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        # Adicionar paginação
        offset = (page - 1) * page_size
        paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"

        # Executar query
        results = pd.read_sql_query(paginated_query, db_connection)

        # Converter resultados para lista de dicionários
        orders = results.to_dict(orient="records")

        # Obter contagem total (sem filtros de paginação)
        total_query = "SELECT COUNT(*) AS total FROM order_items"
        if filters:
            total_query += " WHERE " + " AND ".join(filters)
        total_count = pd.read_sql_query(total_query, db_connection).iloc[0, 0]

        return {
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": (total_count // page_size) + (1 if total_count % page_size > 0 else 0),
            "orders": orders
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar pedidos: {str(e)}")


# Executar o FastAPI localmente (apenas para debug)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)  # Inicia o servidor FastAPI
