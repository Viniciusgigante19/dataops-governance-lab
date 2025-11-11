# correcao_automatica.py

import pandas as pd
import numpy as np
import re
from pathlib import Path
import logging

# -------------------------------------------------
# Configuração de logs
logging.basicConfig(
    filename="logs/correcao_automatica.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------------------------------------
DATA_DIR = Path("data")
OUTPUT_DIR = Path("data_corrigida")
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_FILE = OUTPUT_DIR / "correcao_resumo.txt"

# -------------------------------------------------
def padronizar_dados(df, tipo):
    """Padroniza formatos de colunas genéricas"""
    for col in df.columns:
        # Datas
        if "data" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")

        # Telefones
        if "telefone" in col:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"\D", "", regex=True)
                .str.pad(width=11, side="left", fillchar="0")
            )

        # E-mails
        if "email" in col:
            df[col] = df[col].str.lower().str.strip()
    return df

# -------------------------------------------------
def remover_duplicatas(df, chave):
    """Remove duplicatas com base em lógica inteligente"""
    antes = df.shape[0]
    df = df.sort_values(by=chave).drop_duplicates(subset=chave, keep="last")
    depois = df.shape[0]
    logging.info(f"Removidas {antes - depois} duplicatas baseadas em {chave}")
    return df

# -------------------------------------------------
def preencher_campos_vazios(df, tipo):
    """Preenche campos nulos segundo regras específicas"""
    if tipo == "clientes":
        df["estado"].fillna("SP", inplace=True)
        df["cidade"].fillna("São Paulo", inplace=True)
        df["nome"].fillna("Desconhecido", inplace=True)
    elif tipo == "produtos":
        df["categoria"].fillna("Outros", inplace=True)
        df["ativo"].fillna(True, inplace=True)
    elif tipo == "vendas":
        df["status"].fillna("Pendente", inplace=True)
    elif tipo == "logistica":
        df["status_entrega"].fillna("Em trânsito", inplace=True)
    return df

# -------------------------------------------------
def validar_relacionamentos(vendas, clientes, produtos):
    """Valida foreign keys entre vendas, clientes e produtos"""
    vendas_validas = vendas[
        vendas["id_cliente"].isin(clientes["id_cliente"]) &
        vendas["id_produto"].isin(produtos["id_produto"])
    ]
    removidos = vendas.shape[0] - vendas_validas.shape[0]
    logging.info(f"Removidas {removidos} vendas com FK inválida.")
    return vendas_validas

# -------------------------------------------------
def corrigir_inconsistencias(vendas, logistica):
    """Corrige inconsistências entre datasets"""
    # Exemplo: datas de entrega antes do envio
    mask = logistica["data_entrega_real"] < logistica["data_envio"]
    logistica.loc[mask, "data_entrega_real"] = pd.NaT
    logging.info(f"Corrigidas {mask.sum()} inconsistências de datas de entrega.")
    return vendas, logistica

# -------------------------------------------------
def processar_dataset(nome, chave=None):
    """Pipeline completo de correção de um dataset"""
    df = pd.read_csv(DATA_DIR / f"{nome}.csv", on_bad_lines="skip")
    df = padronizar_dados(df, nome)
    if chave:
        df = remover_duplicatas(df, chave)
    df = preencher_campos_vazios(df, nome)
    df.to_csv(OUTPUT_DIR / f"{nome}_corrigido.csv", index=False)
    logging.info(f"Dataset {nome} processado e salvo.")
    return df

# -------------------------------------------------
def main():
    logging.info("Iniciando correção automática...")
    
    clientes = processar_dataset("clientes", "id_cliente")
    produtos = processar_dataset("produtos", "id_produto")
    vendas = processar_dataset("vendas", "id_venda")
    logistica = processar_dataset("logistica", "id_entrega")
    clientes_lab = processar_dataset("clientes_lab", "id_cliente")

    # Relacionamentos e consistência
    vendas = validar_relacionamentos(vendas, clientes, produtos)
    vendas, logistica = corrigir_inconsistencias(vendas, logistica)

    vendas.to_csv(OUTPUT_DIR / "vendas_corrigido.csv", index=False)
    logistica.to_csv(OUTPUT_DIR / "logistica_corrigido.csv", index=False)

    logging.info("Correção concluída com sucesso.")

# -------------------------------------------------
if __name__ == "__main__":
    main()
