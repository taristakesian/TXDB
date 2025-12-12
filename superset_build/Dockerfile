FROM apache/superset:latest
USER root
# Устанавливаем драйвер в виртуальное окружение, которое использует Superset
RUN pip install --target /app/.venv/lib/python3.10/site-packages psycopg2-binary
USER superset
