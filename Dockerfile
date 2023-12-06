# Build Image
FROM python:3.12-slim-bullseye as build

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /app
COPY ./clinvar_ingest ./clinvar_ingest
COPY pyproject.toml .
RUN python -m pip install .

# Runtime Image
FROM python:3.12-slim-bullseye as runtime
COPY --from=build /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV GCLOUD_PROJECT="clingen-dev"

CMD ["uvicorn", "clinvar_ingest.api.main:app", "--host", "0.0.0.0", "--port", "80", "--log-config", "clinvar_ingest/log_conf.json"]
