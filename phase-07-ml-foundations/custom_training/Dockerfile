# Phase 07 — Custom Training Container
# =====================================
# This Dockerfile packages the XGBoost training script for Vertex AI Custom Training.
# Cloud Build builds this image and pushes it to Artifact Registry (see cloudbuild.yaml).
#
# Vertex AI Custom Training workflow:
#   1. Cloud Build: docker build + push to Artifact Registry
#   2. Vertex AI: pulls the image, spins up compute, runs ENTRYPOINT
#   3. Vertex AI injects AIP_MODEL_DIR (GCS path) and other env vars at startup
#   4. train.py reads the env vars, trains, writes model.pkl to AIP_MODEL_DIR
#   5. Vertex AI registers the model artefact if called from a KFP pipeline
#
# Base image choice:
#   us-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-3:latest
#   - Google-maintained Vertex AI training base image
#   - Pre-installed: Python 3.10, scikit-learn 1.3, numpy, scipy, pandas
#   - Advantage over python:3.10-slim: no need to install numpy/sklearn, saving
#     several hundred MB and ~2 minutes of build time
#   - 'cpu' variant: XGBoost runs on CPU for tabular data (GPU offers no speedup
#     for trees; GPU acceleration matters only for deep learning)

FROM us-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-3:latest

# Set the working directory inside the container.
# All subsequent COPY/RUN commands are relative to /app.
WORKDIR /app

# ── Install dependencies ──────────────────────────────────────────────────────
# Copy requirements.txt first (before train.py) to leverage Docker layer caching:
# if requirements.txt hasn't changed, Docker reuses the cached pip install layer
# even when train.py changes. This keeps CI builds fast (pip install is slow).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# --no-cache-dir: don't write pip's HTTP cache to the image layer.
# Reduces image size by ~100 MB (the cache isn't needed in the container).

# ── Copy training script ──────────────────────────────────────────────────────
# Copy only the training script (not the whole repo).
# Keeping the image minimal reduces pull time on Vertex AI workers.
COPY train.py .

# ── Container entrypoint ──────────────────────────────────────────────────────
# Vertex AI passes CLI arguments after the image URI in the custom training job
# config. ENTRYPOINT ["python", "train.py"] means Vertex AI can append flags:
#   --project my-project --region us-central1 ...
# Using the JSON array form (exec form) avoids a shell wrapper, which ensures
# signals (SIGTERM on job cancellation) reach Python directly.
ENTRYPOINT ["python", "train.py"]
