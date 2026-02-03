# 📊 Python Data Engineering Challenge – JIRA SLA Pipeline

## 📌 Visão Geral
Este projeto implementa um pipeline de dados em Python seguindo o modelo Medalhão (Bronze, Silver e Gold) para ingestão, tratamento e análise de dados fictícios do JIRA, com foco no cálculo de SLA (Service Level Agreement).

O objetivo é demonstrar boas práticas de Data Engineering, organização de código, padronização e clareza para manutenção futura.

---

## 🏗️ Arquitetura do Projeto

### 🔹 Bronze
- Leitura do JSON bruto do JIRA  
- Extração apenas dos campos relevantes  
- Persistência dos dados sem regras de negócio  
- Armazenamento em formato Parquet  

### 🔸 Silver
- Limpeza e padronização dos dados  
- Normalização de nomes de colunas  
- Conversão e padronização de datas  
- Preparação dos dados para regras de negócio  

### 🟡 Gold
- Aplicação das regras de SLA  
- Cálculo do tempo de resolução em horas úteis  
- Verificação de cumprimento de SLA  
- Geração de relatórios analíticos  

---

## 📁 Estrutura de Pastas

```text
project-root/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── sla_calculation.py
│
├── run_pipeline.py
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🐍 Padrões e Convenções
- PEP 8  
- Snake_case para arquivos, funções e variáveis  
- Datas em ISO 8601 (UTC)  
- Booleanos iniciando com `is_`  

---

## ⏱️ Regras de SLA

| Prioridade | SLA Esperado |
|----------|--------------|
| High     | 24 horas     |
| Medium   | 72 horas     |
| Low      | 120 horas    |

- Considera apenas dias úteis  
- Cada dia útil equivale a 24 horas  
- Finais de semana e feriados nacionais são excluídos  

---

## 📈 Relatórios Gerados
- SLA médio por analista  
- SLA médio por tipo de chamado  
- Distribuição de SLA (cumprido vs violado)  

---

## ▶️ Como Executar o Projeto

### Opção recomendada (manual)
```bash
python run_pipeline.py
```

### Execução automática
O script `run_pipeline.py` também valida e instala automaticamente dependências ausentes ao ser executado.

---

## 🚀 Evoluções Futuras
- Testes unitários
- Parametrização externa de SLA
- Orquestração com Airflow ou Prefect

---

## 👤 Autor
Desafio técnico de Data Engineering com foco em boas práticas, clareza e escalabilidade.
