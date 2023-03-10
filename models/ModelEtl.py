from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum


class EnumJumpTypes(Enum):
    byDate = 0
    byID = 1


class ModelEtl(BaseModel):
    id: Optional[int] = None
    from_column: str = Field(..., description="something")
    to_column: str
    from_node_type: str
    to_node_type: str
    edge_formula: str
    relation_type: str
    database_name: str
    table_name: str
    update_at: Optional[str] = None  # not required, can be None
    des: str
    log: Optional[str] = None  # not required, can be None
    log_date: Optional[str] = None  # not required, can be None
    jump_column: str
    jump_type: EnumJumpTypes
    jump_size: int
    jump_start: str
    jump_end: str
    cursor: Optional[int] = None
    busy: Optional[bool] = False
    enabled: bool

    class Config:
        use_enum_values = True
