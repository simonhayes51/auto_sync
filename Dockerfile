FROM mcr.microsoft.com/playwright/python:v1.61.0-noble

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_HEADLESS=true
ENV FUTGG_TEST_LIMIT=10

CMD ["python", "test_futgg_catalogue_10.py"]