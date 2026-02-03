"""Silver layer.

PT: Organiza e limpa os dados da camada Bronze, mantendo todos os chamados (Open, Done, Resolved).
    Salva resultados em Parquet e Excel para verificação.
EN: Organizes and cleans Bronze data, keeping all issues (Open, Done, Resolved).
    Saves results in Parquet and Excel for verification.
"""

# Importa pandas para manipulação de dados tabulares
# Import pandas to handle tabular data
import pandas as pd

# Importa Path para trabalhar com caminhos de arquivos e pastas
# Import Path to work with file and folder paths
from pathlib import Path

# Define caminhos de entrada e saída
# Define input and output paths
BRONZE_FILE = Path("data/bronze/bronze_issues.parquet")   # Arquivo da camada Bronze / Bronze layer file
SILVER_DIR = Path("data/silver")                          # Pasta da camada Silver / Silver layer folder
SILVER_FILE = SILVER_DIR / "silver_issues.parquet"        # Arquivo final em Parquet / Final Parquet file
SILVER_EXCEL = SILVER_DIR / "silver_issues.xlsx"          # Arquivo final em Excel / Final Excel file


def run_silver() -> Path:
    """Reads Bronze, cleans data, and saves Silver parquet + Excel.
    PT: Lê dados da Bronze, limpa e organiza, salva Silver em Parquet + Excel.
    EN: Reads Bronze data, cleans and organizes, saves Silver in Parquet + Excel.
    """

    # 1️ Lê arquivo Parquet da camada Bronze
    # 1️ Read Bronze parquet file
    df = pd.read_parquet(BRONZE_FILE)

    # 2️ Normaliza nomes de colunas para snake_case e aplica regras de nomenclatura
    # 2️ Normalize column names to snake_case and apply naming rules
    df = df.rename(columns={
        "issuesid": "issue_id",                 # ID do chamado / Issue ID
        "issuesissue_type": "issue_type",       # Tipo do chamado / Issue type
        "issuesstatus": "status",               # Status do chamado / Issue status
        "issuespriority": "priority",           # Prioridade / Priority
        "resp_id": "assignee_id",               # ID do responsável / Assignee ID
        "resp_name": "assignee_name",           # Nome do responsável / Assignee name
        "resp_email": "assignee_email",         # Email do responsável / Assignee email
        "timestamps_created_at": "dt_created",  # Data de criação / Creation date
        "timestamps_resolved_at": "dt_resolved" # Data de resolução / Resolution date
    })

    # 3️ Converte colunas de datas para datetime em UTC (ISO 8601)
    # 3️ Convert datetime columns to UTC (ISO 8601)
    for col in ["dt_created", "dt_resolved", "dt_extracted"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # 4️ Converte colunas de texto para Title Case (primeira letra maiúscula)
    # 4️ Convert text columns to Title Case (first letter uppercase)
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(lambda x: x.title() if isinstance(x, str) else x)

    # 5️ Cria pasta Silver se não existir
    # 5️ Create Silver directory if not exists
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # 6️ Salva em Parquet (formato otimizado)
    # 6️ Save as Parquet (optimized format)
    df.to_parquet(SILVER_FILE, index=False)

    # 7️ Salva também em Excel (para verificação)
    # 7️ Save also in Excel (for verification)
    # * Excel não suporta timezone, então removemos tz antes de salvar
    # * Excel does not support timezone, so we remove tz before saving
    for col in ["dt_created", "dt_resolved", "dt_extracted"]:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    #df.to_excel(SILVER_EXCEL, index=False)

    # Mensagens de confirmação no console
    # Confirmation messages in console
    print(f"Silver file generated at: {SILVER_FILE}")
    #print(f"Silver Excel generated at: {SILVER_EXCEL}")

    # Retorna caminho do arquivo Silver gerado
    # Return path of generated Silver file
    return SILVER_FILE


# Executa função principal se arquivo for rodado diretamente
# Run main function if file is executed directly
if __name__ == "__main__":
    run_silver()