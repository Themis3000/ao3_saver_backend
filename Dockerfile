FROM python:3.8-slim-buster

RUN mkdir app
WORKDIR /app

COPY . .

RUN pip3 install -r requirements.txt

CMD ["uvicorn", "main:app"]
