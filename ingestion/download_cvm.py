"""
download_cvm.py — baixa dados públicos de fundos da CVM.
Arquivos disponibilizados como .zip contendo .csv.
Implementa retry automático e skip de arquivos já baixados.
"""
import logging
import time
import zipfile
from pathlib import Path
from datetime import datetime, timedelta
import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
CAD_URL  = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
RAW_DIR  = Path("data/raw")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

def download_file(url: str, dest: Path, retries: int = 3) -> bool:
    """Baixa um arquivo com retry automático."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"Baixando {url} (tentativa {attempt})")
            resp = requests.get(url, timeout=60, headers=HEADERS)
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(resp.content)
            log.info(f"Salvo: {dest} ({len(resp.content)/1024:.0f} KB)")
            return True
        except requests.HTTPError as e:
            log.warning(f"HTTP {e.response.status_code}: {url}")
            if e.response.status_code == 404:
                return False
        except Exception as e:
            log.warning(f"Erro: {e}")
            time.sleep(2)
    return False

def download_informes_diarios(meses: int = 6) -> None:
    """Baixa os informes diários dos últimos N meses (.zip -> .csv)."""
    hoje = datetime.today()
    dest_dir = RAW_DIR / "informes_diarios"
    dest_dir.mkdir(parents=True, exist_ok=True)

    for i in range(meses):
        ref  = hoje.replace(day=1) - timedelta(days=i * 30)
        ym   = ref.strftime('%Y%m')
        csv_dest = dest_dir / f"inf_diario_fi_{ym}.csv"

        if csv_dest.exists():
            log.info(f"Já existe: {csv_dest.name} — pulando")
            continue

        zip_dest = dest_dir / f"inf_diario_fi_{ym}.zip"
        url = f"{BASE_URL}/inf_diario_fi_{ym}.zip"

        if not download_file(url, zip_dest):
            continue

        # Extrair o CSV do zip
        log.info(f"Extraindo {zip_dest.name}...")
        with zipfile.ZipFile(zip_dest, 'r') as z:
            for name in z.namelist():
                if name.endswith('.csv'):
                    z.extract(name, dest_dir)
                    # renomeia para o padrão esperado
                    extracted = dest_dir / name
                    extracted.rename(csv_dest)
                    break

        zip_dest.unlink()  # apaga o zip, só precisamos do csv
        log.info(f"Extraído: {csv_dest.name}")
        time.sleep(0.5)

def download_cadastro() -> None:
    dest = RAW_DIR / "cadastro" / "cad_fi.csv"
    if not dest.exists():
        download_file(CAD_URL, dest)
    else:
        log.info("Cadastro já existe — pulando")

if __name__ == "__main__":
    download_cadastro()
    download_informes_diarios(meses=6)
    log.info("Download concluído.")