FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/
COPY batch/ batch/
COPY scripts/ scripts/

RUN useradd --no-create-home --shell /usr/sbin/nologin appuser
USER appuser

EXPOSE 8000
ENV WORKERS=4
CMD uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers ${WORKERS}
