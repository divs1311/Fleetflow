# Mock driver/vehicle master-data REST API (FastAPI), backed by the deterministic simulator.
FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements/api.lock.txt /tmp/api.lock.txt
RUN pip install --no-cache-dir -r /tmp/api.lock.txt
COPY src/ /app/src/
ENV PYTHONPATH=/app/src PYTHONDONTWRITEBYTECODE=1
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=4)"
CMD ["uvicorn", "porterflow.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
