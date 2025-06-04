FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt pyproject.toml ./

RUN pip install --no-cache-dir -r requirements.txt



CMD ["python", "main.py"]
