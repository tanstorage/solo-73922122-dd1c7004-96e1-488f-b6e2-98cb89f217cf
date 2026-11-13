FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    SAMPLES_DIR=/samples

WORKDIR /app

COPY generate.py ./generate.py
RUN mkdir -p /samples && python generate.py

COPY server.py ./server.py

EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health').status==200 else 1)"

CMD ["python", "server.py"]
