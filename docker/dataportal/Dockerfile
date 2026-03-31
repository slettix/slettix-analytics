FROM python:3.11-slim

WORKDIR /opt/dataportal

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app source is mounted as a volume in development
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090", "--reload"]
