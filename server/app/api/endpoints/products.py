from fastapi import APIRouter, Query, HTTPException
from app.database import db
import pandas as pd
import numpy as np

router = APIRouter()

@router.get("/products")
async def get_products(
    page: int = Query(1, ge=1),  # Página atual, valor mínimo 1
    page_size: int = Query(10, ge=1, le=100),  # Número de itens por página (máx. 100)
    product_id: str = Query(None),  # Filtrar por `product_id`
    category: str = Query(None),  # Filtrar por categoria do produto
    seller_id: str = Query(None)  # Filtrar por vendedor
):
    try:
        # Construir query base
        base_query = """
            SELECT 
                p.*,
                pct.product_category_name_english,
                s.seller_city,
                s.seller_state
            FROM products p
            LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN sellers s ON oi.seller_id = s.seller_id
        """
        filters = []

        # Adicionar filtros com base nos parâmetros
        if product_id:
            filters.append(f"p.product_id = '{product_id}'")
        if category:
            filters.append(f"(p.product_category_name = '{category}' OR pct.product_category_name_english = '{category}')")
        if seller_id:
            filters.append(f"s.seller_id = '{seller_id}'")

        # Concatenar os filtros na query base
        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        # Adicionar paginação
        offset = (page - 1) * page_size
        paginated_query = f"{base_query} GROUP BY p.product_id LIMIT {page_size} OFFSET {offset}"

        # Executar query
        with db.get_cursor() as cursor:
            results = pd.read_sql_query(paginated_query, db.connection)
            
            # Converter tipos numpy para tipos Python nativos
            results = results.replace({np.nan: None})
            for column in results.select_dtypes(include=[np.number]).columns:
                results[column] = results[column].astype(float)

        # Converter resultados para lista de dicionários
        products = results.to_dict(orient="records")

        # Obter contagem total (sem filtros de paginação)
        total_query = """
            SELECT COUNT(DISTINCT p.product_id) AS total 
            FROM products p
            LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN sellers s ON oi.seller_id = s.seller_id
        """
        if filters:
            total_query += " WHERE " + " AND ".join(filters)
        
        with db.get_cursor() as cursor:
            total_count_df = pd.read_sql_query(total_query, db.connection)
            total_count = int(total_count_df.iloc[0, 0])

        return {
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": (total_count // page_size) + (1 if total_count % page_size > 0 else 0),
            "products": products
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar produtos: {str(e)}")

@router.get("/products/{product_id}")
async def get_product_details(product_id: str):
    try:
        # Query para obter detalhes do produto
        product_query = """
            SELECT 
                p.*,
                pct.product_category_name_english
            FROM products p
            LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
            WHERE p.product_id = :product_id
        """

        # Query para obter vendedores do produto
        sellers_query = """
            SELECT DISTINCT
                s.seller_id,
                s.seller_city,
                s.seller_state,
                oi.price,
                oi.freight_value
            FROM order_items oi
            JOIN sellers s ON oi.seller_id = s.seller_id
            WHERE oi.product_id = :product_id
        """
        
        with db.get_cursor() as cursor:
            # Obter detalhes do produto
            product_results = pd.read_sql_query(product_query, db.connection, params={"product_id": product_id})
            
            if product_results.empty:
                raise HTTPException(status_code=404, detail="Produto não encontrado")
            
            # Obter vendedores
            sellers_results = pd.read_sql_query(sellers_query, db.connection, params={"product_id": product_id})
            
            # Converter tipos numpy para tipos Python nativos
            product_results = product_results.replace({np.nan: None})
            sellers_results = sellers_results.replace({np.nan: None})
            
            for df in [product_results, sellers_results]:
                for column in df.select_dtypes(include=[np.number]).columns:
                    df[column] = df[column].astype(float)

        # Converter resultados para dicionários
        product_details = product_results.to_dict(orient="records")[0]
        sellers = sellers_results.to_dict(orient="records")
        
        return {
            "product_details": product_details,
            "sellers": sellers
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar detalhes do produto: {str(e)}")
