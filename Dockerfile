FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir "aiohttp>=3.10"
COPY raft/ raft/
COPY server/ server/
COPY dashboard/ dashboard/
CMD ["python", "-m", "server.node_server"]
