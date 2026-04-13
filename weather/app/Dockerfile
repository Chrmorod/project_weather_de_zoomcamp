FROM python:3.11-slim

WORKDIR /app

COPY app/ /app/

RUN pip install --no-cache-dir \
    streamlit \
    sqlalchemy \
    pandas \
    altair \
    psycopg2-binary \
    pydeck
    
EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
