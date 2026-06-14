FROM python:3.14-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV DENO_PATH=/root/.deno/bin/deno
ENV PATH="/root/.deno/bin:${PATH}"

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg curl ca-certificates unzip \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deno.land/install.sh | sh -s -- -y \
 && ln -sf /root/.deno/bin/deno /usr/local/bin/deno

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=10000
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-10000}"]