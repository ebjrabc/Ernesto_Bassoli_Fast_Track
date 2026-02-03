"""Bronze layer.

PT: Lê o JSON bruto e grava apenas os campos definidos em um DataFrame.
    Premissa: gravar somente em Parquet (mantendo timezone).
EN: Reads raw JSON and writes only selected fields into a DataFrame.
    Premise: save only in Parquet (keeping timezone).
"""

# Permite usar anotações de tipo (type hints) mesmo em versões antigas do Python
# Allow type hints even in older Python versions
from __future__ import annotations

# Importa biblioteca padrão para trabalhar com arquivos JSON
# Import standard library to handle JSON files
import json

# Importa Path para manipular caminhos de arquivos e pastas
# Import Path to handle file and folder paths
from pathlib import Path

# Importa tipos auxiliares para clareza (Dict, List, Any)
# Import helper types for clarity (Dict, List, Any)
from typing import Any, Dict, List

# Importa pandas para manipulação de tabelas (DataFrames)
# Import pandas to handle tabular data (DataFrames)
import pandas as pd

# ============================================================
# Variáveis globais (configurações fixas do script)
# Global variables (fixed script configurations)
# ============================================================

# Caminho do arquivo JSON bruto exportado do Jira
# Path to raw JSON file exported from Jira
INPUT_PATH = Path(r"C:\Users\EBJ\Downloads\projeto\Projeto Fast Track").with_name("jira_issues_raw.json")

# Diretório onde os arquivos da camada Bronze serão salvos
# Directory where Bronze layer files will be saved
BRONZE_DIR = Path("data/bronze")

# Nome do arquivo final em formato Parquet (otimizado para análise)
# Name of final file in Parquet format (optimized for analysis)
OUTPUT_FILE = BRONZE_DIR / "bronze_issues.parquet"

# ============================================================
# Funções auxiliares
# Helper functions
# ============================================================

def read_json_file() -> Dict[str, Any]:
    """
    Lê o arquivo JSON e retorna os dados como um dicionário Python.
    EN: Reads JSON file and returns data as Python dictionary.
    """
    # Abre o arquivo no caminho definido em INPUT_PATH
    # Open file at INPUT_PATH
    with INPUT_PATH.open("r", encoding="utf-8") as f:
        # Converte o conteúdo JSON em objeto Python (dict)
        # Convert JSON content into Python dict
        return json.load(f)


def build_bronze_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Constrói o DataFrame Bronze apenas com os campos definidos.
    EN: Builds Bronze DataFrame only with defined fields.
    """
    # Pega os dados do projeto (parte superior do JSON)
    # Get project data (top-level JSON)
    project = payload.get("project", {})

    # Pega lista de issues (cada issue é um dicionário)
    # Get list of issues (each issue is a dictionary)
    issues: List[Dict[str, Any]] = payload.get("issues", [])

    # Lista que vai armazenar todas as linhas da tabela
    # List to store all table rows
    rows: List[Dict[str, Any]] = []

    # Loop para percorrer cada issue
    # Loop through each issue
    for issue in issues:
        # Pega timestamps (datas de criação e resolução)
        # Get timestamps (creation and resolution dates)
        timestamps = issue.get("timestamps", {})
        if isinstance(timestamps, list) and len(timestamps) > 0:
            ts = timestamps[0]  # Se for lista, pega primeiro / If list, take first
        elif isinstance(timestamps, dict):
            ts = timestamps     # Se for dict, usa direto / If dict, use directly
        else:
            ts = {}             # Se não existir, usa vazio / If not exists, use empty

        # Pega assignee (responsável pela issue)
        # Get assignee (responsible person)
        assignee = issue.get("assignee", [])
        if isinstance(assignee, list) and len(assignee) > 0:
            assignee_info = assignee[0]  # Se for lista, pega primeiro / If list, take first
        elif isinstance(assignee, dict):
            assignee_info = assignee     # Se for dict, usa direto / If dict, use directly
        else:
            assignee_info = {}           # Se não existir, usa vazio / If not exists, use empty

        # Monta dicionário com campos desejados
        # Build dictionary with desired fields
        row = {
            "project_id": project.get("project_id"),           # ID do projeto / Project ID
            "project_name": project.get("project_name"),       # Nome do projeto / Project name
            "dt_extracted": project.get("extracted_at"),       # Data de extração / Extraction date
            "issue_id": issue.get("id"),                       # ID da issue / Issue ID
            "issue_type": issue.get("issue_type"),             # Tipo da issue / Issue type
            "status": issue.get("status"),                     # Status da issue / Issue status
            "priority": issue.get("priority"),                 # Prioridade / Priority
            "assignee_id": assignee_info.get("id"),            # ID do responsável / Assignee ID
            "assignee_name": assignee_info.get("name"),        # Nome do responsável / Assignee name
            "assignee_email": assignee_info.get("email"),      # Email do responsável / Assignee email
            "dt_created": ts.get("created_at"),                # Data de criação / Creation date
            "dt_resolved": ts.get("resolved_at"),              # Data de resolução / Resolution date
        }
        # Adiciona linha na lista
        # Append row to list
        rows.append(row)

    # Converte lista de dicionários em DataFrame pandas
    # Convert list of dictionaries into pandas DataFrame
    df = pd.DataFrame(rows)

    # Converte colunas de datas para datetime UTC (ISO 8601)
    # Convert date columns to datetime UTC (ISO 8601)
    for col in ["dt_extracted", "dt_created", "dt_resolved"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Retorna DataFrame pronto
    # Return prepared DataFrame
    return df

# ============================================================
# Função principal da camada Bronze
# Main function of Bronze layer
# ============================================================

def run_bronze() -> Path:
    """
    Executa a camada Bronze:
    EN: Executes Bronze layer:
    - Lê JSON bruto / Reads raw JSON
    - Constrói DataFrame com campos definidos / Builds DataFrame with defined fields
    - Cria pasta Bronze se não existir / Creates Bronze folder if not exists
    - Grava DataFrame em Parquet / Saves DataFrame in Parquet
    """
    # Lê arquivo JSON bruto
    # Read raw JSON file
    payload = read_json_file()

    # Constrói DataFrame Bronze
    # Build Bronze DataFrame
    df = build_bronze_dataframe(payload)

    # Cria pasta Bronze se não existir
    # Create Bronze folder if not exists
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Grava DataFrame em Parquet (mantendo timezone)
    # Save DataFrame in Parquet (keeping timezone)
    df.to_parquet(OUTPUT_FILE, index=False)

    # Mensagem de confirmação no console
    # Confirmation message in console
    print(f"Bronze file generated at: {OUTPUT_FILE}")

    # Retorna caminho do arquivo gerado
    # Return path of generated file
    return OUTPUT_FILE

# Se script for executado diretamente, roda função principal
# If script is executed directly, run main function
if __name__ == "__main__":
    run_bronze()