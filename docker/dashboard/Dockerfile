# Streamlit dashboard; reads the published serving database (mounted read-only).
FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements/dashboard.lock.txt /tmp/dashboard.lock.txt
RUN pip install --no-cache-dir -r /tmp/dashboard.lock.txt
COPY dashboard/ /app/
ENV PORTERFLOW_DATA_DIR=/data PYTHONDONTWRITEBYTECODE=1
EXPOSE 8501
HEALTHCHECK --interval=15s --timeout=5s --retries=10 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=4)"
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
