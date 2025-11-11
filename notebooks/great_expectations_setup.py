# great_expectations_setup.py

import great_expectations as ge
from great_expectations.data_context import DataContext
from pathlib import Path

# -----------------------------
# Diretórios
DATA_DIR = Path("data")  # datasets já processados

# -----------------------------
# 1️⃣ Configuração do Data Context
def setup_great_expectations_context():
    """
    Configura Data Context do Great Expectations
    Cria datasources para todos os datasets limpos
    """
    context = ge.data_context.DataContext(
        context_root_dir="great_expectations"
    )
    
    # Cria datasources baseados nos CSVs
    for csv_file in DATA_DIR.glob("*.csv"):
        datasource_name = csv_file.stem
        context.add_datasource(
            name=datasource_name,
            class_name="PandasDatasource",
            batch_kwargs_generators={
                "default": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": str(DATA_DIR),
                }
            },
        )
    return context

# -----------------------------
# 2️⃣ Expectation Suite para Clientes
def create_clientes_expectations(validator):
    """
    Cria expectativas para dataset de clientes:
    - Completude: id_cliente, nome, email não nulos
    - Unicidade: id_cliente, email únicos
    - Validade: email formato válido, telefone 11 dígitos
    - Consistência: estado 2 caracteres
    """
    # Completude
    validator.expect_column_values_to_not_be_null("id_cliente")
    validator.expect_column_values_to_not_be_null("nome")
    validator.expect_column_values_to_not_be_null("email")
    
    # Unicidade
    validator.expect_column_values_to_be_unique("id_cliente")
    validator.expect_column_values_to_be_unique("email")
    
    # Validade
    validator.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
    validator.expect_column_values_to_match_regex("telefone", r"^\d{10,11}$")
    
    # Consistência
    validator.expect_column_values_to_match_regex("estado", r"^[A-Z]{2}$")
    
    return validator

# -----------------------------
# Exemplo de uso
if __name__ == "__main__":
    context = setup_great_expectations_context()
    
    # Seleciona CSV de clientes como batch
    batch_kwargs = {"path": str(DATA_DIR / "clientes.csv"), "datasource": "clientes"}
    validator = context.get_validator(batch_kwargs=batch_kwargs, expectation_suite_name="clientes_suite", create_expectation_suite=True)
    
    # Cria expectativas
    validator = create_clientes_expectations(validator)
    
    # Salva a expectation suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    print("Expectation suite de clientes criada com sucesso!")
