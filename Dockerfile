FROM python:3.11-slim-buster

RUN mkdir app
WORKDIR /app

COPY . .

RUN pip3 install -r requirements.txt

CMD ["gunicorn", "main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:80"]
