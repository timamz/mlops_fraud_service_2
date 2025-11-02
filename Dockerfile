FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY app ./app
COPY models ./models
COPY preprocessors ./preprocessors
RUN chmod -R 444 /app/models
RUN mkdir -p /app/input /app/output /app/logs && chmod -R 777 /app/input /app/output /app/logs

VOLUME /app/input
VOLUME /app/output
CMD ["python", "app/app.py"]
