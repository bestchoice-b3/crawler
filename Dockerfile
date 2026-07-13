FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium and its OS-level dependencies
RUN playwright install chromium && playwright install-deps chromium

COPY . .

EXPOSE 3000

CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "3000"]
