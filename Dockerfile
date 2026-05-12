FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY .env.yaml ./
COPY src/ ./src/
COPY config/ ./config/
COPY data/ ./data/
COPY assets/ ./assets/
COPY templates/ ./templates/
COPY pyproject.toml .
RUN pip install -e . --no-deps
ENV PYTHONUNBUFFERED=1
