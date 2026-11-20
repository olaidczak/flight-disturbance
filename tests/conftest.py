import sys
from pathlib import Path

# pozwala na `from common.schema import ...` i `from ingestion import ...` tak jak w Lambdzie
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
