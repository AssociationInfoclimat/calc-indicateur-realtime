FROM ghcr.io/osgeo/gdal:ubuntu-full-3.11.0

WORKDIR /app

# needed for pipx
ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y \
        php \
        php-cli \
        php-common \
        php-mysql \
        python3-pip \
        pipx \
    && pipx install poetry

COPY ./src /app/src
COPY ./calcul_indicateur_rr /app/calcul_indicateur_rr
COPY ./GEOFLA_DEPARTEMENT /app/GEOFLA_DEPARTEMENT
COPY ./pyproject.toml /app/pyproject.toml

# Install dependencies in a virtualenv managed by Poetry
RUN poetry config virtualenvs.in-project true \
    && poetry install --no-interaction --no-ansi --only main

CMD ["php", "/app/src/main.php"]
