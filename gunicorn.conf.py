import os

bind = f"0.0.0.0:{os.environ.get('PORT', '8000')}"
worker_class = "uvicorn_worker.UvicornWorker"
workers = 2
timeout = 120
