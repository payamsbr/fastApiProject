from typing import List
from fastapi import FastAPI
from models.ModelEtl import ModelEtl
from src.db import GraphETLDataBase

app = FastAPI()
db = GraphETLDataBase()


@app.post("/etl")
def create_etl(params: ModelEtl):
    db.create_or_update(params)


@app.patch("/etl/{etl_id}")
def update_etl(params: ModelEtl, etl_id: int):
    db.create_or_update(params, etl_id)


@app.get("/etl", response_model=List[ModelEtl])
def list_etl(page: int):
    return db.list_with_page(page)


@app.get("/")
async def root():
    return "App Working"
