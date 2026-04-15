FROM python:3.11-slim

WORKDIR /opt/dataportal

# requirements.txt ligger i docker/dataportal/ (build context = repo-roten)
COPY docker/dataportal/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kildekode fra dataportal/ og jobs/ bakt inn
# (hostPath fungerer ikke i multi-node kind cluster)
COPY dataportal/ .
COPY jobs/ /opt/dataportal/jobs/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090"]
