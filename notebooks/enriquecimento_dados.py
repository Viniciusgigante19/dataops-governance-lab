# enriquecimento_dados.py
import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data")
OUTPUT_DIR = Path("data/enriquecido")
OUTPUT_DIR.mkdir(exist_ok=True)

def enriquecer_dados():
    """
    Realiza o enriquecimento dos datasets tratados:
      - Geocodificação simulada de endereços
      - Categorização automática de produtos
      - Cálculo de métricas derivadas
      - Flags de qualidade por registro
    """
    print("🔍 Iniciando enriquecimento de dados...")

    # === 1. Carregar dados tratados ===
    clientes = pd.read_csv(DATA_DIR / "clientes_tratado.csv")
    produtos = pd.read_csv(DATA_DIR / "produtos_tratado.csv")
    vendas = pd.read_csv(DATA_DIR / "vendas_tratado.csv")
    logistica = pd.read_csv(DATA_DIR / "logistica_tratado.csv")

    # === 2. Geocodificação simulada ===
    def simular_geocode(estado):
        coordenadas = {
            "SP": (-23.55, -46.63),
            "RJ": (-22.90, -43.20),
            "MG": (-19.92, -43.94),
            "PR": (-25.42, -49.27)
        }
        return coordenadas.get(estado, (0.0, 0.0))

    clientes[["latitude", "longitude"]] = clientes["estado"].apply(
        lambda x: pd.Series(simular_geocode(x))
    )

    # === 3. Categorização automática de produtos ===
    def categorizar(produto):
        p = produto.lower()
        if "tv" in p or "smart" in p: return "Eletrônicos"
        if "notebook" in p or "computador" in p: return "Informática"
        if "camisa" in p or "calça" in p: return "Vestuário"
        return "Outros"

    produtos["categoria_automatica"] = produtos["nome_produto"].apply(categorizar)

    # === 4. Cálculo de métricas derivadas ===
    # Idade do cliente
    clientes["data_nascimento"] = pd.to_datetime(clientes["data_nascimento"], errors="coerce")
    clientes["idade"] = clientes["data_nascimento"].apply(
        lambda x: int((datetime.now() - x).days / 365.25) if pd.notnull(x) else None
    )

    # Tempo de entrega
    logistica["data_envio"] = pd.to_datetime(logistica["data_envio"], errors="coerce")
    logistica["data_entrega"] = pd.to_datetime(logistica["data_entrega"], errors="coerce")
    logistica["tempo_entrega_dias"] = (
        logistica["data_entrega"] - logistica["data_envio"]
    ).dt.days

    # === 5. Flags de qualidade ===
    clientes["flag_qualidade"] = clientes.apply(
        lambda row: "OK" if pd.notnull(row["email"]) and len(str(row["telefone"])) >= 10 else "VERIFICAR",
        axis=1
    )
    produtos["flag_qualidade"] = produtos["preco"].apply(
        lambda x: "OK" if x > 0 else "PREÇO_INVALIDO"
    )
    vendas["flag_qualidade"] = vendas.apply(
        lambda r: "OK" if r["quantidade"] > 0 else "QUANTIDADE_INVALIDA", axis=1
    )

    # === 6. Salvar enriquecidos ===
    clientes.to_csv(OUTPUT_DIR / "clientes_enriquecido.csv", index=False)
    produtos.to_csv(OUTPUT_DIR / "produtos_enriquecido.csv", index=False)
    vendas.to_csv(OUTPUT_DIR / "vendas_enriquecido.csv", index=False)
    logistica.to_csv(OUTPUT_DIR / "logistica_enriquecido.csv", index=False)

    print("✅ Enriquecimento concluído com sucesso.")
    print(f"Arquivos gerados em: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    enriquecer_dados()
