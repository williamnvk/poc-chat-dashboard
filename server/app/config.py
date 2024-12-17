import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    DATABASE_URL = ":memory:"  # Para POC, manter em memória
    CORS_ORIGINS = ["http://localhost:3000"]

settings = Settings() 