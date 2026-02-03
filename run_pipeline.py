"""Pipeline entry point.
Desafio: Ingestão, curadoria e disponibilização de dados fictícios do Jira

PT: Este script garante dependências, define variáveis globais e executa o pipeline Bronze -> Silver -> Gold.
EN: This script ensures dependencies, defines global variables, and runs the Bronze -> Silver -> Gold pipeline.
"""

# PT: Ativa um recurso do Python que trata anotações de tipo como texto (compatibilidade).
# EN: Enables a Python feature that stores type hints as text (compatibility).
from __future__ import annotations

# PT: Importa módulos padrão do Python usados para instalar pacotes e manipular caminhos.
# EN: Imports standard Python modules used to install packages and handle file paths.
import subprocess   # PT: Permite rodar comandos externos (como instalar pacotes). / EN: Allows running external commands (like installing packages).
import sys          # PT: Dá acesso ao interpretador Python em uso. / EN: Provides access to the Python interpreter in use.
import importlib.util  # PT: Usado para verificar se um pacote já está instalado. / EN: Used to check if a package is already installed.
from pathlib import Path  # PT: Facilita trabalhar com caminhos de arquivos/pastas. / EN: Makes it easier to work with file/folder paths.

# ============================================================
# Função para garantir dependências
# Function to ensure dependencies
# ============================================================

def ensure_dependencies() -> None:
    """
    PT: Cria/atualiza requirements.txt e instala dependências necessárias.
    EN: Creates/updates requirements.txt and installs required dependencies.
    """

    # PT: Lista de pacotes externos usados em todo o pipeline.
    # EN: List of external packages used throughout the pipeline.
    required_imports = [
        "pandas",    # PT: Manipulação de tabelas / EN: DataFrame manipulation
        "numpy",     # PT: Cálculos numéricos / EN: Numerical calculations
        "pyarrow",   # PT: Suporte a Parquet / EN: Parquet file support
        "openpyxl",  # PT: Suporte a Excel / EN: Excel file support
        "requests"   # PT: Consumo de API de feriados / EN: Holiday API consumption
    ]

    # PT: Caminho do arquivo requirements.txt (mesma pasta do script).
    # EN: Path to requirements.txt file (same folder as script).
    requirements_path = Path(__file__).with_name("requirements.txt")

    # PT: Cria ou atualiza requirements.txt com os pacotes necessários.
    # EN: Creates or updates requirements.txt with required packages.
    with requirements_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(required_imports))

    # PT: Verifica se cada pacote está instalado, se não estiver instala automaticamente.
    # EN: Checks if each package is installed, if not installs automatically.
    for module_name in required_imports:
        if importlib.util.find_spec(module_name) is None:
            print(f"Installing missing dependency: {module_name}")
            subprocess.check_call([sys.executable, "-m", "pip", "install", module_name])


# PT: Garante que dependências estejam instaladas antes de rodar o pipeline.
# EN: Ensures dependencies are installed before running the pipeline.
ensure_dependencies()

# ============================================================
# Importação das camadas do pipeline
# Import pipeline layers
# ============================================================

# PT: Importa funções que executam cada camada do pipeline.
# EN: Imports functions that run each pipeline layer.
from src.bronze.ingest_bronze import run_bronze
from src.silver.transform_silver import run_silver
from src.gold.build_gold import build_gold

# ============================================================
# Variáveis globais
# Global variables
# ============================================================

# PT: Caminho fixo para o arquivo JSON bruto do Jira.
# EN: Fixed path to the raw Jira JSON file.
INPUT_PATH = Path(r"C:\Users\EBJ\Downloads\projeto\Projeto Fast Track").with_name("jira_issues_raw.json")

# PT: Diretório base onde os dados serão armazenados.
# EN: Base directory where data will be stored.
DATA_DIR = Path("data")

# PT: Diretórios específicos para cada camada.
# EN: Specific directories for each layer.
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# ============================================================
# Função principal
# Main function
# ============================================================

def main() -> None:
    """
    PT: Executa o pipeline completo Bronze -> Silver -> Gold.
    EN: Runs the complete pipeline Bronze -> Silver -> Gold.
    """

    # PT: Verifica se o arquivo de entrada existe.
    # EN: Checks if the input file exists.
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input JSON not found: {INPUT_PATH}")

    # PT: Executa cada camada do pipeline na ordem correta.
    # EN: Runs each pipeline layer in the correct order.
    bronze_path = run_bronze()
    silver_path = run_silver()
    outputs = build_gold()

    # PT: Mostra mensagem final de sucesso e lista os arquivos gerados.
    # EN: Shows final success message and lists generated files.
    print("Pipeline finished successfully ✅")
    print("Generated outputs:")
    #print(f"- Bronze: {bronze_path}")
    #print(f"- Silver: {silver_path}")
    #for name, path in outputs.items():
    #    print(f"- {name}: {path}")


# PT: Garante que o pipeline só rode se o script for chamado diretamente.
# EN: Ensures the pipeline only runs if the script is called directly.
if __name__ == "__main__":
    main()