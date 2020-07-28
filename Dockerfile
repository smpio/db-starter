FROM python:3.7

ENV PYTHONUNBUFFERED=1
ENV DEV_ENV=0
ENV CONTAINER_ENV=1

RUN pip install --upgrade pip

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "-m", "db_starter"]
