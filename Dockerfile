
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim AS runtime

RUN groupadd --gid 1000 pipeline && \
    useradd --uid 1000 --gid pipeline --create-home pipeline

WORKDIR /app

COPY --from=builder /install /usr/local

COPY pipeline/ ./pipeline/

RUN mkdir -p pipeline/logs/dq_reports pipeline/raw && \
    chown -R pipeline:pipeline /app

USER pipeline

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import pandas; import duckdb; print('ok')" || exit 1

ENTRYPOINT ["python"]
CMD ["pipeline/run_pipeline.py"]
