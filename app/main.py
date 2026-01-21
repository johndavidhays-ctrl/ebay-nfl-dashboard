from fastapi import FastAPI
from app.db import init_db, fetch_active_deals

app = FastAPI()

engine = init_db()


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/deals")
def get_deals():
    deals = fetch_active_deals(engine)
    return deals
