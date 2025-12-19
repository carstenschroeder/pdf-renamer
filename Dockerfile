FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pdf_renamer.py .
COPY config.yaml .

CMD ["python", "-u", "pdf_renamer.py"]
