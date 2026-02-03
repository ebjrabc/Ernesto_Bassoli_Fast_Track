"""Gold layer with SLA calculation and reports.

PT: Importa os dados da camada Silver, aplica regras de negócio (SLA),
    gera tabela final com todas as colunas originais + colunas SLA,
    e relatórios agregados em Parquet e Excel.
EN: Imports data from Silver layer, applies business rules (SLA),
    generates final table with all original columns + SLA columns,
    and aggregated reports in Parquet and Excel.
"""

# Importa a biblioteca pandas para manipulação de dados
# Import pandas library to handle dataframes
import pandas as pd

# Importa Path para trabalhar com caminhos de arquivos e pastas
# Import Path to work with file and folder paths
from pathlib import Path

# Importa funções criadas no arquivo sla_calculation.py
# Import functions created in sla_calculation.py
from src.sla_calculation import (
    calculate_resolution_hours_business_days,  # Função que calcula horas úteis / Function to calculate business hours
    get_sla_expected,                          # Função que retorna SLA esperado / Function to return expected SLA
    check_sla_compliance,                      # Função que verifica se SLA foi atendido / Function to check SLA compliance
)

# Define os caminhos dos arquivos de entrada e saída
# Define paths for input and output files
SILVER_FILE = Path("data/silver/silver_issues.parquet")   # Arquivo da camada Silver / Silver layer file
GOLD_DIR = Path("data/gold")                              # Pasta da camada Gold / Gold layer folder
GOLD_FILE = GOLD_DIR / "gold_sla_issues.parquet"          # Arquivo final em Parquet / Final Parquet file
GOLD_EXCEL = GOLD_DIR / "gold_sla_issues.xlsx"            # Arquivo final em Excel / Final Excel file
REPORT_ANALYST = GOLD_DIR / "gold_sla_by_analyst.xlsx"    # Relatório por analista / Report by analyst
REPORT_TYPE = GOLD_DIR / "gold_sla_by_issue_type.xlsx"    # Relatório por tipo de chamado / Report by issue type
REPORT_DISTRIBUTION = GOLD_DIR / "gold_sla_distribution.xlsx"  # Relatório distribuição SLA / SLA distribution report


def build_gold() -> None:
    """Build Gold layer with SLA calculation and reports.
    PT: Constrói a camada Gold com cálculo de SLA e relatórios.
    EN: Builds Gold layer with SLA calculation and reports.
    """

    # 1️ Carrega os dados da camada Silver
    # 1️ Load Silver data
    df = pd.read_parquet(SILVER_FILE)

    # 2️ Filtra apenas chamados resolvidos (Done/Resolved) e com data de resolução preenchida
    # 2️ Keep only resolved issues (Done/Resolved) with dt_resolved not null
    df = df[df["status"].isin(["Done", "Resolved"])].copy()
    df = df[df["dt_resolved"].notna()].copy()

    # 3️ Renomeia colunas de datas para seguir padrão snake_case e prefixo dt_
    # 3️ Rename datetime columns to follow snake_case and dt_ prefix
    df = df.rename(columns={
        "created_at": "dt_created"
    })

    # 4️ Aplica cálculos de SLA e adiciona novas colunas
    # 4️ Apply SLA calculations and add new columns
    df["resolution_hours"] = calculate_resolution_hours_business_days(df)  # Calcula horas úteis / Calculate business hours
    df["sla_expected_hours"] = df["priority"].apply(get_sla_expected)      # SLA esperado por prioridade / Expected SLA by priority
    df["is_sla_met"] = check_sla_compliance(df)                           # Verifica se SLA foi atendido / Check SLA compliance

    # 5️ Converte colunas de datas para formato ISO 8601 UTC (ex: 2025-01-10T08:30:00Z)
    # 5️ Convert date columns to ISO 8601 UTC format
    for col in ["dt_created", "dt_resolved"]:
        df[col] = pd.to_datetime(df[col], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 6️ Cria pasta Gold se não existir e salva tabela final com todas colunas do Silver + SLA
    # 6️ Create Gold folder if not exists and save final table with all Silver columns + SLA
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(GOLD_FILE, index=False)   # Salva em Parquet / Save as Parquet
    
    # Remove timezone from all datetime columns before saving to Excel
    for col in df.select_dtypes(include="datetime64[ns, UTC]").columns:
        df[col] = df[col].dt.tz_localize(None)

    df.to_excel(GOLD_EXCEL, index=False)

    # 7️ Gera relatórios agregados obrigatórios e extras
    # 7️ Generate mandatory and extra aggregated reports

    # Relatório 1: SLA médio por analista
    # Report 1: Average SLA by analyst
    df_by_analyst = df.groupby("assignee_name").agg(
        issue_count=("issue_id", "count"),                # Quantidade de chamados / Number of issues
        avg_resolution_hours=("resolution_hours", "mean") # Média de horas úteis / Average resolution hours
    ).reset_index()
    df_by_analyst.to_excel(REPORT_ANALYST, index=False)

    # Relatório 2: SLA médio por tipo de chamado
    # Report 2: Average SLA by issue type
    df_by_type = df.groupby("issue_type").agg(
        issue_count=("issue_id", "count"),                # Quantidade de chamados / Number of issues
        avg_resolution_hours=("resolution_hours", "mean") # Média de horas úteis / Average resolution hours
    ).reset_index()
    df_by_type.to_excel(REPORT_TYPE, index=False)

    # Relatório 3: Distribuição SLA atendido vs violado
    # Report 3: SLA distribution (Met vs Violated)
    df_distribution = df.groupby("is_sla_met").agg(
        issue_count=("issue_id", "count")                 # Quantidade de chamados por status SLA / Number of issues by SLA status
    ).reset_index()
    df_distribution["percentage"] = (
        df_distribution["issue_count"] / df_distribution["issue_count"].sum() * 100
    ).round(2)                                            # Calcula percentual / Calculate percentage
    df_distribution.to_excel(REPORT_DISTRIBUTION, index=False)

    # Mensagens de confirmação no console
    # Confirmation messages in console
    print(f"Gold file generated at: {GOLD_FILE}")
    print(f"Gold Excel generated at: {GOLD_EXCEL}")
    print(f"Report by analyst generated at: {REPORT_ANALYST}")
    print(f"Report by issue type generated at: {REPORT_TYPE}")
    print(f"Report SLA distribution generated at: {REPORT_DISTRIBUTION}")

    # ✅ Retorna dicionário com todos os arquivos gerados
        # ✅ Retorna dicionário com todos os arquivos gerados
    return {
        "": GOLD_FILE,
    }


# Executa a função principal se o arquivo for rodado diretamente
# Run the main function if the file is executed directly
if __name__ == "__main__":
    build_gold()