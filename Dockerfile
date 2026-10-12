FROM python:3.11-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# El pipeline corre sin privilegios de root
RUN useradd --create-home app && mkdir -p data output && chown -R app:app /app
COPY --chown=app:app . .
USER app

# Imagen de tests: suma pytest, que no hace falta para ejecutar el pipeline
FROM base AS test
COPY --chown=app:app requirements-dev.txt .
RUN pip install --no-cache-dir --user -r requirements-dev.txt
CMD ["python", "-m", "pytest", "tests/", "-v"]

# Imagen del pipeline (es la última etapa, así que es la que se construye por defecto)
FROM base AS etl
CMD ["python", "ecommerce.py"]
