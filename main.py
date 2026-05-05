from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import engine, Base
from app import models
from app.routes import router
from dotenv import load_dotenv
import os
import logging

# Load .env file
load_dotenv()

# RUN THE BACKEND LOCALLY:
#  ../.venv/bin/uvicorn main:app --reload --port 8000
# HOME DIRECTORY .venv/bin/uvicorn backend/main:app --reload --port 8000 --app-dir backend


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("edi")

# Create all tables
Base.metadata.create_all(bind=engine)

RENDER_URL = os.getenv("RENDER_URL", "")

app = FastAPI(
    title="EDI Integration System — FMCG",
    description="Automated Purchase Order Integration for Zepto, Swiggy, Blinkit & more with SAP-style inventory management",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:3000",
        RENDER_URL,                      # Allow Render frontend if deployed
        f"{RENDER_URL}".replace("backend", "frontend"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/")
def root():
    env = os.getenv("ENVIRONMENT", "local")
    return {
        "message": "EDI Integration System is running 🚀",
        "environment": env,
        "docs": "/docs",
        "blinkit_routing": "via_render_proxy" if env == "local" else "direct",
    }
