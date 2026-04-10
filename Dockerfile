FROM python:3.12-bookworm

RUN apt-get update && apt-get install -y wget unzip curl fonts-liberation libappindicator3-1 ... (same Chrome deps as before)

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "app.py"]
