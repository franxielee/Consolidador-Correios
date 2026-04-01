import os
import re
import json
import time
import hashlib
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, StaleElementReferenceException, NoSuchElementException
)

ARC_LOGIN_URL = "https://cas.correios.com.br/login?service=https%3A%2F%2Fapps3.correios.com.br%2Farc%2Fbem-vindo"
ARC_PESQUISA_URL = "https://apps3.correios.com.br/arc/areletronico/pesquisar.html"

EXCEL_PATH = r"C:/Users/XXXXX/Documents/teste correios.xlsx"  # Adicionar User
COL_CODIGOS = 0                      # coluna dos códigos (0 = A)
MAX_LINHAS = 16000                   # limite de leitura do Excel

# BLOCO (ARC aguenta melhor blocos menores)
BLOCK_SIZE = int(os.getenv("ARC_BLOCK_SIZE", "500"))

RETRIES_POR_BLOCO = 3
CHECKPOINT_PATH = "checkpoint_arc.json"

# TEMPOS
TIMEOUT_PADRAO = 90                  # esperas gerais
TIMEOUT_EXPORT = 300                 # liberar botão Exportar
TIMEOUT_DOWNLOAD = 300               # esperar .zip
TIMEOUT_RESULTADOS = int(os.getenv("ARC_TIMEOUT_RESULTADOS", "600"))  # listagem completa
STABILIZE_SECONDS  = int(os.getenv("ARC_STABILIZE_SECONDS", "3"))     # estabilidade

# Formato SRO
SRO_REGEX = re.compile(r"^[A-Z]{2}\d{9}[A-Z]{2}$")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =========================
# Driver Edge com preferências de download
# =========================
def driver_turbinado(download_dir: Optional[str] = None):
    edge_options = Options()
    edge_options.add_argument("--start-maximized")
    edge_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    edge_options.add_experimental_option("useAutomationExtension", False)
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")

    if not download_dir:
        download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(download_dir, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
    }
    edge_options.add_experimental_option("prefs", prefs)

    driver_path = os.getenv("EDGE_DRIVER_PATH")
    if driver_path and os.path.exists(driver_path):
        try:
            service = Service(driver_path, log_output=os.devnull)
            return webdriver.Edge(service=service, options=edge_options)
        except WebDriverException:
            log("Aviso: EDGE_DRIVER_PATH falhou. Tentando Selenium Manager...")

    try:
        service = Service(log_output=os.devnull)
        return webdriver.Edge(options=edge_options, service=service)
    except Exception as e:
        raise RuntimeError(
            "Não consegui inicializar o EdgeDriver. Instale o Edge ou defina EDGE_DRIVER_PATH no .env "
            "apontando para o msedgedriver.exe compatível (veja edge://version)."
        ) from e


# =========================
# Login ARC
# =========================
def login_arc(driver, usuario: str, senha: str):
    log("Acessando tela de login…")
    driver.get(ARC_LOGIN_URL)
    wait = WebDriverWait(driver, TIMEOUT_PADRAO)

    try:
        user = wait.until(EC.presence_of_element_located((By.ID, "username")))
        pwd = driver.find_element(By.ID, "password")
    except TimeoutException:
        raise TimeoutException("Timeout ao localizar campos de login.")

    user.clear(); user.send_keys(usuario)
    pwd.clear();  pwd.send_keys(senha)

    clicou = False
    for locator in [
        (By.XPATH, "//button[@name='submitBtn' and (contains(., 'ENTRAR') or contains(., 'Acessar'))]"),
        (By.XPATH, "//input[@type='submit' and (contains(@value,'ENTRAR') or contains(@value,'Acessar'))]"),
        (By.XPATH, "//button[contains(@type,'submit')]"),
    ]:
        try:
            wait.until(EC.element_to_be_clickable(locator)).click()
            clicou = True
            break
        except Exception:
            continue
    if not clicou:
        raise TimeoutException("Não encontrei o botão de login.")

    log("Aguardando confirmação do login…")
    try:
        WebDriverWait(driver, TIMEOUT_PADRAO).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(@class,'usuario') or contains(.,'ARC') or contains(.,'Bem-vindo')]")
            )
        )
        log("Login realizado com sucesso! 🎉")
    except TimeoutException:
        try:
            erro = driver.find_element(By.XPATH, "//*[contains(text(),'usuário ou senha')] | //*[contains(text(),'invalid')]")
            raise WebDriverException(f"Falha no login: {erro.text.strip()}")
        except NoSuchElementException:
            raise TimeoutException("Timeout esperando a página principal após login.")


def _hash_codigos(codigos: List[str]) -> str:
    h = hashlib.sha256()
    for c in codigos:
        h.update((c or '').strip().encode('utf-8'))
        h.update(b'|')
    return h.hexdigest()


def carregar_checkpoint() -> Dict[str, Any]:
    if os.path.exists(CHECKPOINT_PATH):
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            log("Aviso: falha ao ler checkpoint. Reiniciando do zero.")
    return {"ultimo_bloco_ok": -1, "dataset_hash": None, "total_blocos": None}


def salvar_checkpoint(idx_bloco: int, dataset_hash: str, total_blocos: int) -> None:
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {"ultimo_bloco_ok": idx_bloco, "dataset_hash": dataset_hash, "total_blocos": total_blocos},
            f, ensure_ascii=False, indent=4
        )

def ler_codigos_excel(path: str, col_idx: int, limite: int) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Planilha não encontrada: {path}")

    df = pd.read_excel(path, dtype=str)

    codigos = (
        df.iloc[:limite, col_idx]
        .dropna()
        .astype(str)
        .map(str.strip)
        .tolist()
    )

    validos = [c for c in codigos if SRO_REGEX.match(c)]
    if len(codigos) != len(validos):
        log(f"Aviso: {len(codigos) - len(validos)} códigos removidos (padrão SRO inválido).")
    if not validos:
        raise RuntimeError("Nenhum código válido (SRO) encontrado na planilha.")

    return validos


def split_blocos(codigos: List[str], tamanho: int) -> List[List[str]]:
    return [codigos[i:i + tamanho] for i in range(0, len(codigos), tamanho)]

def aguardar_zip_novo(download_dir: str, inicio_ts: float, timeout: int) -> str:
    log("Aguardando conclusão do download (.zip)…")
    limite = time.time() + timeout
    while time.time() < limite:
        time.sleep(1)
        novos_zips = []
        for nome in os.listdir(download_dir):
            if nome.lower().endswith(".zip"):
                caminho = os.path.join(download_dir, nome)
                try:
                    mtime = os.path.getmtime(caminho)
                    if mtime >= inicio_ts - 5:
                        novos_zips.append((mtime, caminho, nome))
                except FileNotFoundError:
                    continue
        if novos_zips:
            novos_zips.sort(key=lambda x: x[0], reverse=True)
            mtime, caminho, nome = novos_zips[0]
            if os.path.exists(caminho + ".crdownload") or os.path.exists(caminho + ".tmp"):
                continue
            log(f"Download ok: {nome}")
            return caminho
    raise TimeoutException("Tempo limite esperando o .zip.")

def _aguardar_listagem_completa(driver, esperado: int):
    """
    Considera a interface do ARC:
      - badge verde no botão Exportar: .badge1-green[data-badge="N"]
      - texto 'Resultados: 1 – X de N'
    Espera N == len(bloco) e estabilidade por STABILIZE_SECONDS.
    """
    deadline = time.time() + TIMEOUT_RESULTADOS
    estabilidade_desde = None
    ultimo_visto = None

    def ler_total() -> Optional[int]:
        # 1) Badge verde (mais confiável)
        try:
            badge = driver.find_element(By.CSS_SELECTOR, ".badge1-green[data-badge]")
            valor = badge.get_attribute("data-badge") or (badge.text or "").strip()
            if valor and valor.isdigit():
                return int(valor)
        except Exception:
            pass

        # 2) Texto "Resultados: 1 – 5 de 200"
        try:
            el = driver.find_element(By.XPATH, "//*[contains(text(),'Resultados') and contains(text(),'de')]")
            texto = (el.text or "").strip()
            m = re.search(r"de\s+(\d+)", texto)
            if m:
                return int(m.group(1))
        except Exception:
            pass

        return None

    while time.time() < deadline:
        atual = ler_total()
        if atual == esperado:
            if estabilidade_desde is None:
                estabilidade_desde = time.time()
            elif time.time() - estabilidade_desde >= STABILIZE_SECONDS:
                log(f"Listagem completa detectada: {atual}/{esperado}.")
                return
        else:
            estabilidade_desde = None

        ultimo_visto = atual
        time.sleep(0.5)

    raise TimeoutException(
        f"O ARC não estabilizou a listagem completa dentro de {TIMEOUT_RESULTADOS}s "
        f"(último_visto={ultimo_visto}, esperado={esperado})."
    )

def _esperar_export_pronto(driver) -> None:
    wait_long = WebDriverWait(driver, TIMEOUT_EXPORT)
    try:
        wait_long.until(EC.presence_of_element_located((By.ID, "botao-exportar")))
        wait_long.until(EC.element_to_be_clickable((By.ID, "botao-exportar")))
    except TimeoutException:
        raise TimeoutException("Botão 'Exportar' não ficou disponível a tempo.")

def processar_bloco(driver, bloco: List[str], idx_bloco: int, total_blocos: int):
    log(f"=== Bloco {idx_bloco + 1}/{total_blocos} | {len(bloco)} códigos ===")
    wait = WebDriverWait(driver, TIMEOUT_PADRAO)

    driver.get(ARC_PESQUISA_URL)
    wait.until(EC.presence_of_element_located((By.ID, "txtListaObjetos")))

    textarea = driver.find_element(By.ID, "txtListaObjetos")
    textarea.clear()
    textarea.send_keys(";\n".join(bloco))

    clicked = False
    for locator in [
        (By.XPATH, "//a[contains(@class,'botao-principal') and (contains(.,'Mostrar último evento') or contains(.,'Mostrar último'))]"),
        (By.XPATH, "//a[contains(@class,'botao-principal')]"),
    ]:
        for _ in range(3):
            try:
                WebDriverWait(driver, 15).until(EC.element_to_be_clickable(locator)).click()
                clicked = True
                break
            except (TimeoutException, StaleElementReferenceException):
                time.sleep(1)
                continue
            except NoSuchElementException:
                break
        if clicked:
            break

    if not clicked:
        raise TimeoutException("Não consegui acionar a pesquisa ('Mostrar último evento').")

    # 1) Botão Exportar habilitado (servidor terminou a primeira fase)
    _esperar_export_pronto(driver)
    # 2) Total exibido (badge/“Resultados: … de N”) bate com len(bloco) e estabiliza
    _aguardar_listagem_completa(driver, esperado=len(bloco))

    log("Resultados completos e estáveis. Iniciando exportação…")

    inicio_download = time.time()
    for _ in range(3):
        try:
            driver.find_element(By.ID, "botao-exportar").click()
            break
        except StaleElementReferenceException:
            time.sleep(0.5)
        except Exception as e:
            raise WebDriverException(f"Falha ao clicar no botão 'Exportar': {e}")
    else:
        raise WebDriverException("Não foi possível clicar em 'Exportar' após múltiplas tentativas.")

    log("Exportação iniciada. 📥")
    download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
    zip_path = aguardar_zip_novo(download_dir, inicio_download, TIMEOUT_DOWNLOAD)
    log(f"ZIP baixado: {zip_path}")

def main():
    load_dotenv()
    usuario = os.getenv("USUARIO_CORREIOS")
    senha = os.getenv("SENHA_CORREIOS")
    if not usuario or not senha:
        raise RuntimeError("Defina USUARIO_CORREIOS e SENHA_CORREIOS no .env")

    try:
        codigos = ler_codigos_excel(EXCEL_PATH, COL_CODIGOS, MAX_LINHAS)
    except Exception as e:
        raise RuntimeError(f"Erro na leitura da planilha: {e}")

    tamanho_bloco = max(1, BLOCK_SIZE)
    blocos = split_blocos(codigos, tamanho_bloco)
    total_blocos = len(blocos)
    dataset_hash = _hash_codigos(codigos)

    log(f"Total de códigos válidos: {len(codigos)} | blocos: {total_blocos} (tam={tamanho_bloco})")

    ckpt = carregar_checkpoint()
    ultimo_ok = int(ckpt.get("ultimo_bloco_ok", -1))
    ckpt_hash = ckpt.get("dataset_hash")

    if ckpt_hash != dataset_hash or ckpt.get("total_blocos") != total_blocos:
        if ckpt_hash is not None:
            log("Mudança de dados/blocos detectada. Reiniciando do início. ⚠️")
        ultimo_ok = -1

    salvar_checkpoint(ultimo_ok, dataset_hash, total_blocos)
    start_bloco = ultimo_ok + 1

    if start_bloco >= total_blocos:
        log(f"🏁 Nada a retomar: execuções anteriores concluídas.")
        log("Para reprocessar, apague o checkpoint_arc.json.")
        return
    elif start_bloco > 0:
        log(f"Retomando a partir do bloco {start_bloco + 1} de {total_blocos}…")

    driver = None
    try:
        driver = driver_turbinado()
        login_arc(driver, usuario, senha)

        for idx in range(start_bloco, total_blocos):
            tentativas = 0
            while tentativas < RETRIES_POR_BLOCO:
                try:
                    processar_bloco(driver, blocos[idx], idx, total_blocos)
                    salvar_checkpoint(idx, dataset_hash, total_blocos)
                    break
                except Exception as e:
                    tentativas += 1
                    log(f"❌ Falha no bloco {idx + 1} (Tentativa {tentativas}/{RETRIES_POR_BLOCO}): {type(e).__name__} - {e}")
                    if tentativas < RETRIES_POR_BLOCO:
                        espera = 5 * tentativas
                        log(f"Tentando de novo em {espera}s…")
                        time.sleep(espera)
                        try:
                            driver.get(ARC_PESQUISA_URL)
                        except Exception:
                            pass
                    else:
                        log(f"⛔ Bloco {idx+1} falhou após {tentativas} tentativas. Interrompendo.")
                        raise

        log("🏁 Todos os blocos foram processados com sucesso!")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        log("Driver encerrado.")


if __name__ == "__main__":
    main()
