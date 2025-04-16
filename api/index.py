from main import app  # import FastAPI app from main.py
from mangum import Mangum

handler = Mangum(app)