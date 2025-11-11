# pipeline_ingestao.py

import pandas as pd
from pathlib import Path
import logging

# -----------------------------
# Configuração de diretórios
DATASET_DIR = Path("datasets")  # arquivos originais
OUTPUT_DIR = Path("data")       # arquivos processados
OUTPUT_DIR.mkdir(exist_ok=True)

# -----------------------------
# Configuração de logs
logging.basicConfig(
    filename=OUTPUT_DIR / "pipeline_ingestao.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_clientes(df):
    # Corrige codificação
    df['nome'] = df['nome'].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df['cidade'] = df['cidade'].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    # Remove duplicatas
    df = df.drop_duplicates(subset='id_cliente', keep='first')

    # Valida e-mails
    df = df[df['email'].notna() & df['email'].str.contains(r"[^@]+@[^@]+\.[^@]+")]

    # Garante nome e telefone
    df = df[df['nome'].notna() & df['telefone'].notna()]

    # Padroniza datas
    for col in ['data_nascimento', 'data_cadastro']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df = df.dropna(subset=['data_nascimento', 'data_cadastro'])

    logging.info(f"clientes.csv - registros finais: {df.shape[0]}")
    return df

def clean_clientes_lab(df):
    # Corrige codificação
    df['nome'] = df['nome'].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    # Remove duplicatas
    df = df.drop_duplicates(subset='id_cliente', keep='first')

    # Valida e-mails
    df = df[df['email'].notna() & df['email'].str.contains(r"[^@]+@[^@]+\.[^@]+")]

    # Idade válida
    df = df[df['idade'].notna() & (df['idade'] >= 0) & (df['idade'] < 120)]

    # Status preenchido
    df = df[df['status'].notna()]

    # Datas
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
    df = df.dropna(subset=['data_cadastro'])

    logging.info(f"clientes_lab.csv - registros finais: {df.shape[0]}")
    return df

def clean_produtos(df):
    # Corrige codificação
    df['nome_produto'] = df['nome_produto'].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df['categoria'] = df['categoria'].astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    # Remove duplicatas
    df = df.drop_duplicates(subset='id_produto', keep='first')

    # Preço e estoque válidos
    df = df[df['preco'] >= 0]
    df = df[df['estoque'] >= 0]

    # Data
    df['data_criacao'] = pd.to_datetime(df['data_criacao'], errors='coerce')
    df = df.dropna(subset=['data_criacao'])

    # Ativo: bool
    df['ativo'] = df['ativo'].astype(bool)

    logging.info(f"produtos.csv - registros finais: {df.shape[0]}")
    return df

def clean_vendas(df, clientes_ids, produtos_ids):
    # Remove duplicatas
    df = df.drop_duplicates(subset='id_venda', keep='first')

    # Valida referências
    df = df[df['id_cliente'].isin(clientes_ids)]
    df = df[df['id_produto'].isin(produtos_ids)]

    # Quantidade e valores válidos
    df = df[df['quantidade'] > 0]
    df = df[df['valor_unitario'] >= 0]
    df = df[df['valor_total'] >= 0]

    # Datas
    df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
    df = df.dropna(subset=['data_venda'])

    logging.info(f"vendas.csv - registros finais: {df.shape[0]}")
    return df

def clean_logistica(df, vendas_ids):
    # Remove duplicatas
    df = df.drop_duplicates(subset='id_entrega', keep='first')

    # Valida referência à venda
    df = df[df['id_venda'].isin(vendas_ids)]

    # Datas
    for col in ['data_envio', 'data_entrega_prevista', 'data_entrega_real']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    logging.info(f"logistica.csv - registros finais: {df.shape[0]}")
    return df

# -----------------------------
# Pipeline de ingestão

try:
    # Carregamento
    clientes = pd.read_csv(DATASET_DIR / "clientes.csv", encoding='utf-8', on_bad_lines='skip')
    clientes_lab = pd.read_csv(DATASET_DIR / "clientes_lab.csv", encoding='utf-8', on_bad_lines='skip')
    produtos = pd.read_csv(DATASET_DIR / "produtos.csv", encoding='utf-8', on_bad_lines='skip')
    vendas = pd.read_csv(DATASET_DIR / "vendas.csv", encoding='utf-8', on_bad_lines='skip')
    logistica = pd.read_csv(DATASET_DIR / "logistica.csv", encoding='utf-8', on_bad_lines='skip')

    # Limpeza
    clientes = clean_clientes(clientes)
    clientes_lab = clean_clientes_lab(clientes_lab)
    produtos = clean_produtos(produtos)
    vendas = clean_vendas(vendas, clientes_ids=set(clientes['id_cliente']), produtos_ids=set(produtos['id_produto']))
    logistica = clean_logistica(logistica, vendas_ids=set(vendas['id_venda']))

    # Salva todos em /data
    clientes.to_csv(OUTPUT_DIR / "clientes.csv", index=False)
    clientes_lab.to_csv(OUTPUT_DIR / "clientes_lab.csv", index=False)
    produtos.to_csv(OUTPUT_DIR / "produtos.csv", index=False)
    vendas.to_csv(OUTPUT_DIR / "vendas.csv", index=False)
    logistica.to_csv(OUTPUT_DIR / "logistica.csv", index=False)

    logging.info("Pipeline de ingestão finalizado com sucesso.")

except Exception as e:
    logging.error(f"Erro no pipeline de ingestão: {e}")
    raise
