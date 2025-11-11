# dashboard_qualidade.py
"""
Dashboard de Qualidade - TechCommerce

Funcionalidades:
- Configura ou reutiliza Great Expectations Data Context em ./great_expectations
- Executa validações (se houver expectation suites)
- Constrói Data Docs (HTML gerado pelo GE)
- Calcula métricas agregadas de qualidade por dataset
- Gera um relatório HTML executivo customizado (TechCommerce)
- Tenta exportar o relatório para PDF (weasyprint ou pdfkit)
- Log detalhado das operações em data/dashboard_qualidade.log
"""

import os
import sys
import shutil
import logging
from pathlib import Path
import json
from datetime import datetime

import pandas as pd

# Tentativas opcionais de dependências para export PDF
try:
    from weasyprint import HTML
    _HAS_WEASYPRINT = True
except Exception:
    _HAS_WEASYPRINT = False

try:
    import pdfkit
    _HAS_PDFKIT = True
except Exception:
    _HAS_PDFKIT = False

# Great Expectations
try:
    import great_expectations as ge
    from great_expectations.data_context import DataContext
    _HAS_GREAT_EXPECTATIONS = True
except Exception:
    _HAS_GREAT_EXPECTATIONS = False

# -------------------------
# Paths (seguindo seu contrato: originais em /datasets, processados em /data)
ROOT_DIR = Path.cwd()
DATA_DIR = Path("data")              # deve conter os datasets processados
DOCS_DIR = DATA_DIR / "quality_docs"  # saída de docs e relatórios
GE_DIR = Path("great_expectations")   # data context do GE

DOCS_DIR.mkdir(parents=True, exist_ok=True)
(Path("logs")).mkdir(exist_ok=True)

# Logging
LOG_FILE = DOCS_DIR / "dashboard_qualidade.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# -------------------------
def setup_great_expectations_context():
    """
    Inicializa ou recupera o DataContext do Great Expectations.
    Retorna o objeto DataContext ou None se GE não estiver disponível.
    """
    if not _HAS_GREAT_EXPECTATIONS:
        logging.warning("Great Expectations não está instalado. Pulando configuração do Data Context.")
        return None

    try:
        # Se já existe um contexto, abra. Caso contrário, cria um novo minimalista.
        if GE_DIR.exists() and (GE_DIR / "great_expectations.yml").exists():
            logging.info("Reutilizando Great Expectations Data Context existente em %s", str(GE_DIR.resolve()))
            context = DataContext(context_root_dir=str(GE_DIR))
        else:
            logging.info("Criando novo Great Expectations Data Context em %s", str(GE_DIR.resolve()))
            context = ge.data_context.DataContext.create(project_root_dir=str(GE_DIR))
            # Cria datasources simples baseados em CSVs em /data (PandasDatasource)
            # Não sobrescreve se já existirem datasources
            # We'll add a single PandasDatasource for CSVs in /data
            try:
                context.add_datasource(
                    name="csv_files",
                    class_name="PandasDatasource",
                    batch_kwargs_generators={
                        "default": {
                            "class_name": "SubdirReaderBatchKwargsGenerator",
                            "base_directory": str(DATA_DIR.resolve())
                        }
                    }
                )
                logging.info("Datasource 'csv_files' criado apontando para %s", str(DATA_DIR.resolve()))
            except Exception as e:
                logging.warning("Falha ao adicionar datasource: %s", e)
        return context
    except Exception as e:
        logging.error("Erro ao configurar Great Expectations Data Context: %s", e)
        return None

# -------------------------
def run_validations_for_all_suites(context):
    """
    Para cada expectation suite no contexto, tenta validar o correspondente arquivo CSV em /data.
    Retorna um dicionário com resultados por suite.
    """
    results = {}
    if context is None:
        logging.info("Context é None. Nenhuma validação será executada.")
        return results

    try:
        suites = context.list_expectation_suites()
    except Exception as e:
        logging.warning("Não foi possível listar expectation suites: %s", e)
        return results

    # batch selection: iremos procurar arquivos CSV com nomes correspondentes aos suites (heurística)
    for suite in suites:
        suite_name = suite.expectation_suite_name
        logging.info("Processando expectation suite: %s", suite_name)
        # heurística: se existe data/<suite_name>.csv usar como batch
        candidate_path = DATA_DIR / f"{suite_name}.csv"
        if not candidate_path.exists():
            # tenta sem sufixo "_suite" e sem "suite"
            alt = candidate_path
            if candidate_path.name.endswith("_suite.csv"):
                alt = DATA_DIR / candidate_path.name.replace("_suite.csv", ".csv")
            if not alt.exists():
                logging.info("Arquivo para suite %s não encontrado em %s. Pulando.", suite_name, str(DATA_DIR.resolve()))
                continue
            candidate_path = alt

        try:
            batch_kwargs = {"path": str(candidate_path.resolve()), "datasource": "csv_files"}
            # get_validator pode ser lançado se não houver datasource/esquema; tentar de forma segura
            validator = context.get_validator(batch_kwargs=batch_kwargs, expectation_suite_name=suite_name, create_expectation_suite=False)
            validation_result = validator.validate()
            results[suite_name] = validation_result
            # Salva resultado JSON em docs
            out_json = DOCS_DIR / f"validation_result_{suite_name}.json"
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(validation_result.to_json_dict(), f, ensure_ascii=False, indent=2)
            logging.info("Validação concluída para %s. Resultado salvo em %s", suite_name, str(out_json))
        except Exception as e:
            logging.error("Erro ao validar suite %s: %s", suite_name, e)
    return results

# -------------------------
def build_great_expectations_datadocs(context):
    """
    Constrói Data Docs (HTML) via Great Expectations.
    Devolve o caminho onde os docs foram construídos ou None.
    """
    if context is None:
        logging.info("Context é None. Skip build_data_docs.")
        return None

    try:
        context.build_data_docs()  # gera os HTMLs no diretório definido no context
        docs_sites = context.get_docs_sites_urls()
        logging.info("Data Docs gerados. Sites: %s", docs_sites)
        return docs_sites
    except Exception as e:
        logging.error("Erro ao construir Data Docs: %s", e)
        return None

# -------------------------
def compute_dataset_metrics():
    """
    Calcula métricas de qualidade e dimensões para todos os CSVs em /data.
    Retorna um dict com resumos por dataset.
    """
    metrics = {}
    csv_files = sorted([p for p in DATA_DIR.glob("*.csv")])
    if not csv_files:
        logging.warning("Nenhum CSV encontrado em %s", str(DATA_DIR.resolve()))
        return metrics

    for csv in csv_files:
        name = csv.stem
        try:
            df = pd.read_csv(csv, on_bad_lines="skip")
            total = len(df)
            nulls = df.isnull().sum().to_dict()
            duplicates = df.duplicated().sum()
            cols = list(df.columns)
            # exemplos de métricas específicas
            metric = {
                "arquivo": str(csv),
                "linhas": int(total),
                "colunas": len(cols),
                "colunas_lista": cols,
                "nulls_por_coluna": {k: int(v) for k, v in nulls.items()},
                "duplicatas": int(duplicates),
            }
            # métricas adicionais heurísticas
            if "id_cliente" in df.columns:
                metric["clientes_unicos"] = int(df["id_cliente"].nunique())
            if "preco" in df.columns:
                try:
                    metric["preco_min"] = float(df["preco"].min())
                    metric["preco_max"] = float(df["preco"].max())
                except Exception:
                    pass

            metrics[name] = metric
            logging.info("Métricas calculadas para %s: linhas=%d, duplicatas=%d", name, total, duplicates)
        except Exception as e:
            logging.error("Erro ao calcular métricas para %s: %s", str(csv), e)
    return metrics

# -------------------------
def render_executive_html(metrics, ge_docs_urls=None, output_path=DOCS_DIR / "executive_report.html"):
    """
    Renderiza um HTML executivo com:
      - Cabeçalho TechCommerce
      - Sumário de métricas por dataset
      - Links/embeds para Data Docs gerados pelo GE (quando disponível)
      - Seções com resultados de validação (se existentes)
    """
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    title = "TechCommerce - Relatório Executivo de Qualidade de Dados"
    header = f"""
    <div style="font-family: Arial, sans-serif; margin: 20px;">
      <h1 style="color:#1f4e79;">{title}</h1>
      <p>Gerado em: {now}</p>
      <hr/>
    </div>
    """

    # Metrícas
    tables_html = ""
    for name, m in metrics.items():
        tables_html += f"<h2 style='font-family: Arial, sans-serif;'>{name}</h2>"
        tables_html += "<ul style='font-family: Arial, sans-serif;'>"
        tables_html += f"<li>Linhas: {m.get('linhas')}</li>"
        tables_html += f"<li>Colunas: {m.get('colunas')}</li>"
        tables_html += f"<li>Duplicatas: {m.get('duplicatas')}</li>"
        # nulls
        nulls = m.get("nulls_por_coluna", {})
        if nulls:
            tables_html += "<li>Nulls por coluna:<ul>"
            for c, n in nulls.items():
                tables_html += f"<li>{c}: {n}</li>"
            tables_html += "</ul></li>"
        tables_html += "</ul>"

    # Great Expectations links (se disponível)
    ge_section = ""
    if ge_docs_urls:
        ge_section += "<h2>Great Expectations - Data Docs</h2>"
        ge_section += "<ul>"
        # ge_docs_urls pode ser um dict ou lista; normalizar
        try:
            if isinstance(ge_docs_urls, dict):
                for site_name, url in ge_docs_urls.items():
                    ge_section += f"<li>{site_name}: <a href='{url}' target='_blank'>{url}</a></li>"
            elif isinstance(ge_docs_urls, list):
                for url in ge_docs_urls:
                    ge_section += f"<li><a href='{url}' target='_blank'>{url}</a></li>"
        except Exception:
            ge_section += "<li>Data Docs gerados (ver pasta great_expectations). Verifique localmente.</li>"
        ge_section += "</ul>"

    # Monta HTML final
    html = f"""
    <html>
      <head>
        <meta charset="utf-8"/>
        <title>{title}</title>
      </head>
      <body>
        {header}
        <div style="margin: 20px;">
          <h2 style="font-family: Arial, sans-serif;">Sumário de Métricas</h2>
          {tables_html}
          {ge_section}
        </div>
      </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logging.info("Relatório executivo HTML gerado em %s", str(output_path))
    return output_path

# -------------------------
def export_html_to_pdf(html_path: Path, pdf_path: Path):
    """
    Tenta exportar HTML para PDF usando weasyprint ou pdfkit (wkhtmltopdf).
    Retorna True se sucesso, False caso contrário.
    """
    if _HAS_WEASYPRINT:
        try:
            HTML(filename=str(html_path)).write_pdf(str(pdf_path))
            logging.info("Exportado PDF via weasyprint em %s", str(pdf_path))
            return True
        except Exception as e:
            logging.error("weasyprint erro: %s", e)

    if _HAS_PDFKIT:
        try:
            pdfkit.from_file(str(html_path), str(pdf_path))
            logging.info("Exportado PDF via pdfkit em %s", str(pdf_path))
            return True
        except Exception as e:
            logging.error("pdfkit erro: %s", e)

    logging.warning("Nenhuma biblioteca de conversão para PDF disponível (weasyprint/pdfkit). Não foi possível gerar PDF.")
    return False

# -------------------------
def main():
    logging.info("Iniciando dashboard_qualidade.py")

    # 1) Setup Great Expectations (opcional)
    context = setup_great_expectations_context()

    # 2) Run validations if expectation suites exist
    validation_results = {}
    if context is not None:
        try:
            validation_results = run_validations_for_all_suites(context)
        except Exception as e:
            logging.error("Erro ao executar validações: %s", e)

    # 3) Build Data Docs (GE)
    ge_docs_urls = None
    if context is not None:
        try:
            ge_docs_urls = build_great_expectations_datadocs(context)
        except Exception as e:
            logging.error("Erro ao construir Data Docs: %s", e)

    # 4) Compute dataset metrics
    metrics = compute_dataset_metrics()

    # 5) Render executive HTML report (custom TechCommerce template)
    html_report = render_executive_html(metrics, ge_docs_urls=ge_docs_urls)

    # 6) Try export to PDF
    pdf_report = DOCS_DIR / "executive_report.pdf"
    exported = export_html_to_pdf(html_report, pdf_report)
    if exported:
        logging.info("Relatório executivo PDF disponível em %s", str(pdf_report))
    else:
        logging.info("Relatório PDF não gerado. Verifique dependências ou abra o HTML em um navegador.")

    logging.info("dashboard_qualidade finalizado. Logs em %s", str(LOG_FILE.resolve()))

if __name__ == "__main__":
    main()
