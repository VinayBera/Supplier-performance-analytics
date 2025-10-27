import duckdb, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DB = DATA / "opspulse.duckdb"

def run(cmd):
    print(">", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    run([sys.executable, str(ROOT / "etl" / "generate_data.py")])
    run([sys.executable, str(ROOT / "etl" / "transform_validate.py")])

    con = duckdb.connect(str(DB))
    con.execute((ROOT / "models" / "sql" / "ddl.sql").read_text())
    con.execute((ROOT / "models" / "sql" / "kpis.sql").read_text())
    con.close()
    print(f"[DB] DuckDB created at {DB}")

    run([sys.executable, str(ROOT / "ml" / "train_late_model.py")])
    print("[DONE] Pipeline completed successfully.")

if __name__ == "__main__":
    main()
