FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

EXPOSE 8000
# 演练全部走容器内 loopback 的真实 UDP; 仅暴露 FastAPI 控制面
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
