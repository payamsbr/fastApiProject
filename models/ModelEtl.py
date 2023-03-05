from pydantic import BaseModel, Field


class ModelEtl(BaseModel):
    id: int | None = None
    from_column: str = Field(..., description="something")
    to_column: str
    from_node_type: str
    to_node_type: str
    edge_formula: str
    relation_type: str
    table_name: str
    datetime_column: str
    update_at: str | None = None  # not required, can be None
    des: str
    log: str | None = None  # not required, can be None
    log_date: str | None = None  # not required, can be None
    start_date: str
    end_date: str
    update_interval: int
    enabled: int
