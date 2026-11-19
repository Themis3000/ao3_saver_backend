FROM python:3.14
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./ /code
CMD ["fastapi", "run", "main.py", "--port", "80", "--workers", "6", "--proxy-headers"]
