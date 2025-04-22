from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
from sqlalchemy import text
from dotenv import load_dotenv
load_dotenv()
import app.models

DEFAULT_DATABASE_URL = os.getenv(
    "DEFAULT_DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/postgres"
)
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/temporal_vault"
)
default_engine=create_engine(DEFAULT_DATABASE_URL,isolation_level="AUTOCOMMIT")
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def init_db():
    with default_engine.connect() as conn:
        try:
            conn.execute(text("CREATE DATABASE temporal_vault"))
            print("Database 'temporal_vault' created successfully!")
        except Exception as e:
            print("Database already exists:")
       
    
    global engine
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    print(f"connected to database:{engine.url}")
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
