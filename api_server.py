from fastapi import FastAPI
import subprocess
import time
import sys
import os

app = FastAPI()


@app.get("/")
def home():
    return {"status": "API is running"}


@app.get("/run-crawl")
def run_crawl(group_url: str):
    start = time.time()

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    result = subprocess.run(
        [sys.executable, "main.py", group_url],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )

    end = time.time()

    return {
        "status": "success" if result.returncode == 0 else "error",
        "duration": f"{end - start:.2f}s",
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }
