from fastapi import APIRouter, Query, HTTPException
from app.database import db
from app.models.schemas import OrderFilters
import pandas as pd
import numpy as np

router = APIRouter()

@router.get("/orders")
async def get_orders(
    page: int = Query(1, ge=1),  # Página atual, valor mínimo 1
    page_size: int = Query(10, ge=1, le=100),  # Número de itens por página (máx. 100)
    order_id: str = Query(None),  # Filtrar por `order_id`
    product_id: str = Query(None),  # Filtrar por `product_id`
    customer_id: str = Query(None),  # Filtrar por `customer_id`
    product_category: str = Query(None)  # Filtrar por categoria do produto
):
    try:
        # Construir query base
        base_query = """
            SELECT o.*, oi.*, p.product_category_name, pct.product_category_name_english 
            FROM orders o 
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN products p ON oi.product_id = p.product_id
            LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
        """
        filters = []

        # Adicionar filtros com base nos parâmetros
        if order_id:
            filters.append(f"o.order_id = '{order_id}'")
        if product_id:
            filters.append(f"oi.product_id = '{product_id}'")
        if customer_id:
            filters.append(f"o.customer_id = '{customer_id}'")
        if product_category:
            filters.append(f"(p.product_category_name = '{product_category}' OR pct.product_category_name_english = '{product_category}')")

        # Concatenar os filtros na query base
        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        # Adicionar paginação
        offset = (page - 1) * page_size
        paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"

        # Executar query
        with db.get_cursor() as cursor:
            results = pd.read_sql_query(paginated_query, db.connection)
            
            # Converter tipos numpy para tipos Python nativos
            results = results.replace({np.nan: None})
            for column in results.select_dtypes(include=[np.number]).columns:
                results[column] = results[column].astype(float)

        # Converter resultados para lista de dicionários
        orders = results.to_dict(orient="records")

        # Obter contagem total (sem filtros de paginação)
        total_query = """
            SELECT COUNT(*) AS total 
            FROM orders o 
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN products p ON oi.product_id = p.product_id
            LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
        """
        if filters:
            total_query += " WHERE " + " AND ".join(filters)
        
        with db.get_cursor() as cursor:
            total_count_df = pd.read_sql_query(total_query, db.connection)
            total_count = int(total_count_df.iloc[0, 0])  # Converter para int Python nativo

        return {
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": (total_count // page_size) + (1 if total_count % page_size > 0 else 0),
            "orders": orders
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar pedidos: {str(e)}")

@router.get("/orders/{order_id}")
async def get_order_details(order_id: str):
    try:
        # Query to get order details
        order_query = """
            SELECT 
                o.*,
                c.customer_city,
                c.customer_state,
                r.review_score,
                r.review_comment_title,
                r.review_comment_message
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN order_reviews r ON o.order_id = r.order_id
            WHERE o.order_id = :order_id
        """

        # Query to get order items with product and seller details
        items_query = """
            SELECT 
                oi.order_item_id,
                oi.product_id,
                oi.seller_id,
                oi.shipping_limit_date,
                oi.price,
                oi.freight_value,
                p.product_category_name,
                pct.product_category_name_english,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                s.seller_city,
                s.seller_state
            FROM order_items oi
            LEFT JOIN products p ON oi.product_id = p.product_id 
            LEFT JOIN sellers s ON oi.seller_id = s.seller_id
            LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
            WHERE oi.order_id = :order_id
        """

        # Query to get payment details
        payments_query = """
            SELECT
                payment_sequential,
                payment_type,
                payment_installments,
                payment_value
            FROM order_payments
            WHERE order_id = :order_id
        """
        
        with db.get_cursor() as cursor:
            # Get order details
            order_results = pd.read_sql_query(order_query, db.connection, params={"order_id": order_id})
            
            if order_results.empty:
                raise HTTPException(status_code=404, detail="Order not found")
            
            # Get order items
            items_results = pd.read_sql_query(items_query, db.connection, params={"order_id": order_id})

            # Get payment details
            payments_results = pd.read_sql_query(payments_query, db.connection, params={"order_id": order_id})
            
            # Convert numpy types to native Python types
            order_results = order_results.replace({np.nan: None})
            items_results = items_results.replace({np.nan: None})
            payments_results = payments_results.replace({np.nan: None})
            
            for df in [order_results, items_results, payments_results]:
                for column in df.select_dtypes(include=[np.number]).columns:
                    df[column] = df[column].astype(float)

        # Convert results to dictionaries
        order_details = order_results.to_dict(orient="records")[0]  # Get first row since it's a single order
        order_items = items_results.to_dict(orient="records")
        order_payments = payments_results.to_dict(orient="records")
        
        return {
            "order_details": order_details,
            "items": order_items,
            "payments": order_payments
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching order details: {str(e)}")
