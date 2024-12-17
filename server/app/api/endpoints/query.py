import os
from fastapi import APIRouter, HTTPException
from app.models.schemas import QueryRequest
from app.database import db
import openai
import json
from app.config import settings  # Certifique-se de que você está importando as configurações

router = APIRouter()

# Defina a chave da API do OpenAI
openai.api_key = settings.OPENAI_API_KEY  # Carregue a chave da API do arquivo .env

@router.post("/query")
async def query_all_tables(request: QueryRequest):
    try:
        # Detectar tabelas no banco de dados
        with db.get_cursor() as cursor:
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
        with db.get_cursor() as cursor:
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar a consulta: {str(e)}") 