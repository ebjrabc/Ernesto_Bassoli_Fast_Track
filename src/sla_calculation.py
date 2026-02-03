"""SLA calculation utilities (business days only, full-day hours).

PT: Utilitários para cálculo de SLA considerando apenas dias úteis (24h por dia),
    excluindo finais de semana e feriados nacionais.
EN: SLA calculation utilities considering only business days (24h per day),
    excluding weekends and national holidays.
"""

# Importa pandas para manipulação de dados tabulares
# Import pandas to handle tabular data
import pandas as pd

# Importa numpy para cálculos numéricos eficientes
# Import numpy for efficient numerical calculations
import numpy as np

# Importa requests para consumir API de feriados
# Import requests to consume holiday API
import requests

# Importa datetime para manipulação de datas
# Import datetime to handle dates
from datetime import datetime

# URL da API pública de feriados nacionais do Brasil
# Public API URL for Brazilian national holidays
HOLIDAYS_API = "https://brasilapi.com.br/api/feriados/v1/{year}"


def get_holidays(years: list[int]) -> set:
    """Fetch national holidays for given years.
    PT: Busca feriados nacionais para os anos informados.
    EN: Fetch national holidays for the given years.
    """
    holidays = set()  # Cria conjunto vazio para armazenar feriados / Create empty set to store holidays
    for year in years:  # Itera sobre cada ano / Iterate over each year
        response = requests.get(HOLIDAYS_API.format(year=year))  # Chama API / Call API
        for h in response.json():  # Itera sobre resposta JSON / Iterate over JSON response
            # Converte string de data para objeto date e adiciona ao conjunto
            # Convert date string to date object and add to set
            holidays.add(datetime.strptime(h["date"], "%Y-%m-%d").date())
    return holidays  # Retorna conjunto de feriados / Return set of holidays


def calculate_resolution_hours_business_days(df: pd.DataFrame) -> pd.Series:
    """
    Calculate resolution time in business hours (24h per business day),
    excluding weekends and national holidays.
    PT: Calcula tempo de resolução em horas úteis (24h por dia útil),
        excluindo finais de semana e feriados nacionais.
    """

    # Converte colunas de datas para datetime em UTC
    # Convert date columns to datetime in UTC
    df["dt_created"] = pd.to_datetime(df["dt_created"], utc=True)
    df["dt_resolved"] = pd.to_datetime(df["dt_resolved"], utc=True)

    # Coleta todos os anos presentes nas datas para buscar feriados
    # Collect all years present in dates to fetch holidays
    years = list(set(df["dt_created"].dt.year) | set(df["dt_resolved"].dt.year))
    holidays = get_holidays(years)

    # Cria intervalo de dias úteis entre menor data de criação e maior data de resolução
    # Create business day range between min creation date and max resolution date
    bdays = pd.bdate_range(df["dt_created"].min().normalize(),
                           df["dt_resolved"].max().normalize(),
                           freq="C", holidays=holidays)
    bday_set = set(bdays.date)  # Conjunto de dias úteis válidos / Set of valid business days

    def compute_hours(start, end):
        # Se alguma data for nula, retorna NaN
        # If any date is null, return NaN
        if pd.isna(start) or pd.isna(end):
            return np.nan

        # Lista de dias entre início e fim
        # List of days between start and end
        business_days = pd.date_range(start.normalize(), end.normalize(), freq="D")

        # Filtra apenas dias úteis que não sejam feriados
        # Filter only business days excluding holidays
        valid_days = [d.date() for d in business_days if d.weekday() < 5 and d.date() not in holidays]

        # Cada dia útil vale 24 horas
        # Each business day counts as 24 hours
        return len(valid_days) * 24

    # Aplica função compute_hours linha a linha e retorna série com horas de resolução
    # Apply compute_hours row by row and return series with resolution hours
    return df.apply(lambda row: compute_hours(row["dt_created"], row["dt_resolved"]), axis=1)


def get_sla_expected(priority: str) -> int:
    """Return SLA expected hours based on priority.
    PT: Retorna SLA esperado em horas com base na prioridade.
    EN: Return expected SLA hours based on priority.
    """
    # Mapeamento de prioridade para horas de SLA
    # Mapping priority to SLA hours
    mapping = {"High": 24, "Medium": 72, "Low": 120}
    return mapping.get(str(priority).title(), np.nan)  # Retorna valor ou NaN / Return value or NaN


def check_sla_compliance(df: pd.DataFrame) -> pd.Series:
    """Check if SLA was met (returns 'Atendido' or 'Violado').
    PT: Verifica se SLA foi atendido (retorna 'Atendido' ou 'Violado').
    EN: Check if SLA was met (returns 'Atendido' or 'Violado').
    """
    # Compara horas de resolução com SLA esperado
    # Compare resolution hours with expected SLA
    return np.where(
        df["resolution_hours"] <= df["sla_expected_hours"],  # Condição / Condition
        "Atendido",  # SLA atendido / SLA met
        "Violado"    # SLA violado / SLA violated
    )