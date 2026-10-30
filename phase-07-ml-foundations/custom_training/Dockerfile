FROM us-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-3:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train.py .

ENTRYPOINT ["python", "train.py"]
