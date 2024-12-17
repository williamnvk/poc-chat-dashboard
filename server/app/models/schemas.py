from pydantic import BaseModel
from typing import Optional, List

class QueryRequest(BaseModel):
    question: str

class OrderFilters(BaseModel):
    page: int = 1
    page_size: int = 10
    order_id: Optional[str] = None
    product_id: Optional[str] = None
    customer_id: Optional[str] = None 