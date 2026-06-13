# Use Astral's official uv image — Python 3.12 on Debian slim, with uv pre-installed.
# Why: bypasses pip entirely, reproducible cross-platform builds (M-series Mac, Windows WSL, x86 Linux).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# All app code and venv live under /opt/dagster/app (matches docker-compose volume mounts)
WORKDIR /opt/dagster/app

# --- Dependency layer (cached unless pyproject.toml or uv.lock change) ---
# Copy only the lock files first so dependency installation is cached separately
# from app code changes. This makes rebuilds ~instant when only Python/SQL changes.
COPY pyproject.toml uv.lock ./

# --frozen: don't update uv.lock; fail if it would change. Enforces reproducibility.
# --no-install-project: install dependencies but not the project itself yet (no app code present).
RUN uv sync --frozen --no-install-project

# --- App code layer ---
COPY pipelines/ pipelines/
COPY dbt_project/ dbt_project/

# Re-run sync now that the project's own files exist (no-op for deps, just registers project)
RUN uv sync --frozen

# Dagster needs DAGSTER_HOME to be set, and the directory must exist
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

# Make the venv's executables (dagster, dbt, etc.) discoverable, and make pipelines/ importable
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"
ENV PYTHONPATH="/opt/dagster/app:$PYTHONPATH"

EXPOSE 3000

# `dagster dev` is for local development — auto-reloads code, runs in-process.
# For production you'd use `dagster-webserver` + `dagster-daemon` separately.
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "-m", "pipelines.definitions"]
