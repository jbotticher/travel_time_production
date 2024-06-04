FROM python:3.9-slim-bookworm

WORKDIR /app

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "project.pipelines.travel_time_etl"]