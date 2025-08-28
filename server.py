# server.py (psycopg3-compatible)
import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text, Column, Integer, String, DateTime, Text, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

# ----- Database URL normalization -----
raw_url = os.getenv("DATABASE_URL", "")
if not raw_url:
    raise RuntimeError("Set env var DATABASE_URL to your Postgres connection string (Neon).")

# Force SSL
if "sslmode=" not in raw_url:
    sep = "&" if "?" in raw_url else "?"
    raw_url = f"{raw_url}{sep}sslmode=require"

# Use psycopg3 driver explicitly to avoid psycopg2 on Python 3.13
if raw_url.startswith("postgresql://"):
    DATABASE_URL = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
else:
    DATABASE_URL = raw_url

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
Base = declarative_base()

# ---------- Models ----------
class Counter(Base):
    __tablename__ = "counters"
    type = Column(String, primary_key=True)
    next = Column(Integer, nullable=False, default=1)
    prefix = Column(String, nullable=False, default="")
    padding = Column(Integer, nullable=False, default=0)

class Client(Base):
    __tablename__ = "clients"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    address = Column(Text, nullable=True)
    email = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    __table_args__ = (UniqueConstraint("name", "email", name="uq_clients_name_email"),)

class History(Base):
    __tablename__ = "history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=False, default=datetime.utcnow)
    type = Column(String, nullable=False)
    number = Column(String, nullable=False)
    client = Column(String, nullable=True)
    total_ttc = Column(String, nullable=True)
    path = Column(Text, nullable=True)

Base.metadata.create_all(engine)

# ---------- Schemas ----------
class CounterIn(BaseModel):
    next: int = 1
    prefix: str = ""
    padding: int = 0

class CounterOut(BaseModel):
    type: str
    next: int
    prefix: str
    padding: int

class TakeNextIn(BaseModel):
    doc_type: str = Field(..., alias="doc_type")

class TakeNextOut(BaseModel):
    formatted: str

class ClientIn(BaseModel):
    name: str
    address: Optional[str] = ""
    email: Optional[str] = ""
    phone: Optional[str] = ""

class ClientOut(ClientIn):
    id: int

class HistoryIn(BaseModel):
    type: str
    number: str
    client: Optional[str] = ""
    total_ttc: Optional[str] = ""
    path: Optional[str] = ""

class HistoryOut(HistoryIn):
    id: int
    ts: datetime

# ---------- App ----------
app = FastAPI(title="AFCOTEC Mini-API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------
def formatted_number(next_val: int, prefix: str, padding: int) -> str:
    if padding and next_val >= 0:
        num = f"{next_val:0{padding}d}"
    else:
        num = str(next_val)
    return f"{prefix}{num}"

# ---------- Endpoints ----------
@app.post("/take_next_number", response_model=TakeNextOut)
def take_next_number(payload: TakeNextIn):
    doc_type = payload.doc_type
    with SessionLocal() as db:
        db.execute(text("BEGIN"))
        row = db.execute(text("SELECT type, next, prefix, padding FROM counters WHERE type=:t FOR UPDATE"), {"t": doc_type}).fetchone()
        if not row:
            db.execute(text("INSERT INTO counters(type,next,prefix,padding) VALUES (:t,1,'',0)"), {"t": doc_type})
            next_val, prefix, padding = 1, "", 0
        else:
            _, next_val, prefix, padding = row
        formatted = formatted_number(next_val, prefix, padding)
        db.execute(text("UPDATE counters SET next=:n WHERE type=:t"), {"n": next_val + 1, "t": doc_type})
        db.commit()
        return {"formatted": formatted}

@app.get("/counters", response_model=list[CounterOut])
def list_counters():
    with SessionLocal() as db:
        rows = db.execute(text("SELECT type, next, prefix, padding FROM counters ORDER BY type")).fetchall()
        return [{"type": r[0], "next": r[1], "prefix": r[2], "padding": r[3]} for r in rows]

@app.put("/counters/{doc_type}", response_model=CounterOut)
def set_counter(doc_type: str, c: CounterIn):
    with SessionLocal() as db:
        db.execute(text("""
            INSERT INTO counters(type,next,prefix,padding) VALUES (:t,:n,:p,:pad)
            ON CONFLICT (type) DO UPDATE SET next=EXCLUDED.next, prefix=EXCLUDED.prefix, padding=EXCLUDED.padding
        """), {"t": doc_type, "n": c.next, "p": c.prefix, "pad": c.padding})
        db.commit()
        return {"type": doc_type, **c.model_dump()}

@app.get("/clients", response_model=list[ClientOut])
def list_clients():
    with SessionLocal() as db:
        rows = db.execute(text("SELECT id,name,address,email,phone FROM clients ORDER BY name")).fetchall()
        return [{"id": r[0], "name": r[1], "address": r[2] or "", "email": r[3] or "", "phone": r[4] or ""} for r in rows]

@app.post("/clients", response_model=ClientOut)
def upsert_client(c: ClientIn):
    with SessionLocal() as db:
        try:
            row = db.execute(text("SELECT id FROM clients WHERE name=:n AND email=:e"), {"n": c.name, "e": c.email}).fetchone()
            if row:
                db.execute(text("""
                    UPDATE clients SET address=:a, phone=:ph WHERE id=:id
                """), {"a": c.address or "", "ph": c.phone or "", "id": row[0]})
                db.commit()
                cid = row[0]
            else:
                r = db.execute(text("""
                    INSERT INTO clients(name,address,email,phone) VALUES (:n,:a,:e,:ph) RETURNING id
                """), {"n": c.name, "a": c.address or "", "e": c.email or "", "ph": c.phone or ""}).fetchone()
                db.commit()
                cid = r[0]
            newrow = db.execute(text("SELECT id,name,address,email,phone FROM clients WHERE id=:i"), {"i": cid}).fetchone()
            return {"id": newrow[0], "name": newrow[1], "address": newrow[2] or "", "email": newrow[3] or "", "phone": newrow[4] or ""}
        except IntegrityError:
            db.rollback()
            raise HTTPException(400, "Client unique constraint failed")

@app.post("/history", response_model=HistoryOut)
def add_history(h: HistoryIn):
    with SessionLocal() as db:
        r = db.execute(text("""
            INSERT INTO history(ts,type,number,client,total_ttc,path)
            VALUES (:ts,:ty,:num,:cl,:ttc,:p) RETURNING id, ts
        """), {"ts": datetime.utcnow(), "ty": h.type, "num": h.number, "cl": h.client or "", "ttc": h.total_ttc or "", "p": h.path or ""}).fetchone()
        db.commit()
        return {"id": r[0], "ts": r[1], **h.model_dump()}

@app.get("/history", response_model=list[HistoryOut])
def list_history(limit: int = 200):
    with SessionLocal() as db:
        rows = db.execute(text("""
            SELECT id, ts, type, number, client, total_ttc, path
            FROM history ORDER BY ts DESC LIMIT :lim
        """), {"lim": limit}).fetchall()
        return [{"id": r[0], "ts": r[1], "type": r[2], "number": r[3], "client": r[4], "total_ttc": r[5], "path": r[6]} for r in rows]
