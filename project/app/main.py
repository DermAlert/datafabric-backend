from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware


# from app.database.populate_db import populate_db

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     pass

# app = FastAPI(lifespan=lifespan)
app = FastAPI()

origins = [
    "http://localhost:8081", 
  
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def healthcheck():
    return {"message": "Project Running Successfully"}


