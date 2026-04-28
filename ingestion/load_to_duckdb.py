"""
load_to_duckdb.py — carrega CSVs da CVM no DuckDB.
DuckDB lê CSVs nativamente — muito mais rápido que pandas para grandes volumes.
"""
import logging
from pathlib import Path
import duckdb

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = Path("data/cvm_fundos.duckdb")
RAW_DIR = Path("data/raw")

def load_cadastro(con: duckdb.DuckDBPyConnection) -> None:
    csv = RAW_DIR / "cadastro" / "cad_fi.csv"
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_cadastro_fundos AS
        SELECT * FROM read_csv('{csv}',
            delim=';',
            header=true,
            encoding='cp1252'
        )
    """)
    n = con.execute("SELECT COUNT(*) FROM raw_cadastro_fundos").fetchone()[0]
    log.info(f"Cadastro carregado: {n:,} fundos")

def load_informes(con: duckdb.DuckDBPyConnection) -> int:
    """Carrega todos os CSVs de informes diários com deduplicação."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_informes_diarios (
            TP_FUNDO_CLASSE VARCHAR,
            CNPJ_FUNDO_CLASSE VARCHAR,
            ID_SUBCLASSE      VARCHAR,
            DT_COMPTC         DATE,
            VL_TOTAL          DOUBLE,
            VL_QUOTA          DOUBLE,
            VL_PATRIM_LIQ     DOUBLE,
            CAPTC_DIA         DOUBLE,
            RESG_DIA          DOUBLE,
            NR_COTST          INTEGER
        )
    """)
    csvs = sorted((RAW_DIR / "informes_diarios").glob("*.csv"))
    for csv in csvs:
        mes = csv.stem.split("_")[-1]
        existe = con.execute("""
            SELECT COUNT(*) FROM raw_informes_diarios
            WHERE strftime('%Y%m', DT_COMPTC) = ?
        """, [mes]).fetchone()[0]
        if existe > 0:
            log.info(f"Mês {mes} já carregado — pulando")
            continue
        log.info(f"Carregando {csv.name}...")
        con.execute(f"""
            INSERT INTO raw_informes_diarios
            SELECT
                TP_FUNDO_CLASSE,
                CNPJ_FUNDO_CLASSE,
                ID_SUBCLASSE,
                TRY_CAST(DT_COMPTC    AS DATE),
                TRY_CAST(VL_TOTAL     AS DOUBLE),
                TRY_CAST(VL_QUOTA     AS DOUBLE),
                TRY_CAST(VL_PATRIM_LIQ AS DOUBLE),
                TRY_CAST(CAPTC_DIA    AS DOUBLE),
                TRY_CAST(RESG_DIA     AS DOUBLE),
                TRY_CAST(NR_COTST     AS INTEGER)
            FROM read_csv('{csv}',
                delim=';',
                header=true,
                encoding='cp1252',
                null_padding=true
            )
        """)
        log.info(f"Mês {mes} carregado.")

    count = con.execute(
        "SELECT COUNT(*) FROM raw_informes_diarios"
    ).fetchone()[0]
    log.info(f"Total de registros no banco: {count:,}")
    return count

if __name__ == "__main__":
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    load_cadastro(con)
    load_informes(con)
    con.close()
    log.info("Banco pronto.")