# Ejemplo: producer/Dockerfile, consumer/Dockerfile, ui/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Optimiza instalación de dependencias Prometheus
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# puerto Prometheus metrics
EXPOSE 8000  

CMD ["python", "app.py"]
