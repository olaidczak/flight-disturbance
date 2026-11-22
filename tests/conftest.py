import sys
from pathlib import Path

# Allows `from common.schema import ...` and `from ingestion import ...` just like in AWS Lambda
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
