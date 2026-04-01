import os, glob, csv, zipfile, tempfile, io
import pandas as pd

# === CONFIG ===
base_path  = r"C:\Users\XXXXXX\Downloads" // ADICIONAR O USER
nome_base  = "XX_XX_XXXX"// ALTERAR A DATA ATUAL
saida_csv  = os.path.join(base_path, f"{nome_base}_consolidado.csv")
fazer_dedup = False  

def detect_encoding(path):
    # tentativa simples: utf-8-sig -> utf-8 -> latin1
    for enc in ("utf-8-sig","utf-8","latin1"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.read(4096)
            return enc
        except Exception:
            continue
    return "latin1"

def sniff_sep(text_sample):
    # tenta ; , | \t
    cand = [';', ',', '|', '\t']
    try:
        dialect = csv.Sniffer().sniff(text_sample, delimiters=';,|\t')
        if dialect.delimiter in cand:
            return dialect.delimiter
    except Exception:
        pass
    counts = {d: text_sample.count(d) for d in cand}
    return max(counts, key=counts.get)

def read_csv_resiliente(path_csv):
    """
    1) Lê primeira linha (cabeçalho) para obter nomes e número de colunas (n).
    2) Lê o restante com engine='python' permitindo colunas variáveis.
    3) Se vierem colunas > n, rejunta extras na última coluna.
    Retorna DataFrame com exatamente n colunas e nomes corretos.
    """
    enc = detect_encoding(path_csv)
    with open(path_csv, "r", encoding=enc, errors="ignore") as f:
        sample = f.read(4096)
        f.seek(0)
        header_line = f.readline().rstrip("\n\r")
    sep = sniff_sep(sample)

    # nomes do cabeçalho
    header_cols = [h.strip() for h in header_line.split(sep)]
    n = len(header_cols)

    # lê arquivo inteiro novamente, pulando cabeçalho pois vamos impor nomes
    with open(path_csv, "r", encoding=enc, errors="ignore") as f:
        content = f.read()

    # cria um buffer sem a primeira linha
    content_no_header = "\n".join(content.splitlines()[1:])
    buf = io.StringIO(content_no_header)

    # lê com muitas colunas “provisórias”
    tmp = pd.read_csv(
        buf,
        sep=sep,
        engine="python",
        header=None,
        dtype=str,
        na_filter=False,
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        on_bad_lines="skip"  # se houver linha totalmente irrecuperável
    )

    if tmp.empty:
        # retorna DF vazio com as colunas corretas
        return pd.DataFrame(columns=header_cols), enc, sep

    # garante colunas suficientes
    if tmp.shape[1] < n:
        # completa com colunas vazias
        for _ in range(n - tmp.shape[1]):
            tmp[tmp.shape[1]] = ""
    elif tmp.shape[1] > n:
        # rejunta extras na última coluna
        base = tmp.iloc[:, :n-1].copy()
        resto = tmp.iloc[:, n-1:].astype(str)
        last = resto.apply(lambda r: sep.join([x for x in r if x != ""]), axis=1)
        tmp = pd.concat([base, last.rename(n-1)], axis=1)

    tmp.columns = header_cols
    # tira espaços dos nomes
    tmp.columns = [c.strip() for c in tmp.columns]
    return tmp, enc, sep

# === execução ===
zip_paths = sorted(glob.glob(os.path.join(base_path, f"{nome_base}*.zip")))
if not zip_paths:
    print("⚠ Nenhum .zip encontrado.")
    raise SystemExit

dfs = []
total_bruto = 0
relato = []

for zp in zip_paths:
    if not zipfile.is_zipfile(zp):
        print(f"⏭ Ignorado: {os.path.basename(zp)}")
        continue
    with tempfile.TemporaryDirectory() as td:
        try:
            with zipfile.ZipFile(zp, "r") as z:
                z.extractall(td)
        except Exception as e:
            print(f"❌ Erro extraindo {os.path.basename(zp)}: {e}")
            continue

        achou = False
        for root,_,files in os.walk(td):
            for fn in files:
                if fn.lower().endswith(".csv"):
                    achou = True
                    path = os.path.join(root, fn)
                    try:
                        df, enc, sep = read_csv_resiliente(path)
                        nlin = len(df)
                        total_bruto += nlin
                        dfs.append(df)
                        print(f"✅ {os.path.basename(zp)} -> {fn} | {nlin} linhas | enc={enc} | sep='{sep}'")
                        relato.append((os.path.basename(zp), fn, nlin, enc, sep, "OK"))
                    except Exception as e:
                        print(f"❌ {os.path.basename(zp)} -> {fn}: {e}")
                        relato.append((os.path.basename(zp), fn, 0, None, None, f"ERRO {e}"))
        if not achou:
            print(f"⚠ {os.path.basename(zp)}: nenhum CSV encontrado.")

if not dfs:
    print("⚠ Nada consolidado.")
    raise SystemExit

df_final = pd.concat(dfs, ignore_index=True, sort=False)
df_final.dropna(how="all", inplace=True)

antes = len(df_final)
removidas = 0
if fazer_dedup:
    df_final.drop_duplicates(inplace=True)
    removidas = antes - len(df_final)

# salva sempre com ponto-e-vírgula
df_final.to_csv(saida_csv, sep=";", index=False, encoding="utf-8-sig")

print("\n===== RESUMO =====")
print(f"Zips lidos: {len(zip_paths)}")
print(f"Linhas brutas somadas: {total_bruto}")
print(f"Total no consolidado: {len(df_final)}")
if fazer_dedup:
    print(f"Duplicadas removidas: {removidas}")
print(f"Arquivo final: {saida_csv}")
