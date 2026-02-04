
# Projeto de Engenharia de Dados – Pipeline Medallion com Azure Blob Storage (JIRA SLA)

## Visão Geral

Este projeto implementa um **pipeline profissional de Engenharia de Dados em Python**, seguindo a **Arquitetura Medallion (Bronze, Silver e Gold)**, com o objetivo de processar dados de chamados do **JIRA** e calcular indicadores de **SLA (Service Level Agreement)**.

O pipeline é totalmente automatizado e contempla:
- Ingestão de dados a partir do **Azure Blob Storage** (ou arquivo local como fallback);
- Organização e tratamento dos dados em camadas bem definidas;
- Aplicação de regras de negócio para cálculo de SLA;
- Geração de arquivos analíticos e relatórios prontos para uso em **Excel**.

Este README foi escrito de forma **didática**, permitindo que pessoas sem conhecimento técnico em Python ou Engenharia de Dados consigam compreender o funcionamento e o valor do projeto.

---

## O que é um Pipeline de Dados?

Um pipeline de dados é um fluxo automatizado que:
1. Coleta dados de uma fonte;
2. Estrutura e limpa esses dados;
3. Aplica regras de negócio;
4. Entrega informações confiáveis para análise e tomada de decisão.

Neste projeto, todo esse processo ocorre ao executar um único comando.

---

## Arquitetura do Pipeline – Medallion

### 🥉 Bronze – Ingestão de Dados Brutos
- Leitura de um arquivo JSON de chamados do JIRA;
- Fonte principal: **Azure Blob Storage** (autenticação via Service Principal);
- Fonte alternativa: arquivo local para desenvolvimento;
- Nenhuma regra de negócio aplicada;
- Objetivo: preservar os dados conforme recebidos.

**Saída:**
- `data/bronze/bronze_issues.parquet`

---

### 🥈 Silver – Dados Limpos e Normalizados
- Padronização de nomes de colunas;
- Normalização de textos;
- Conversão e tratamento de datas;
- Preparação dos dados para análises e regras de negócio.

**Saídas:**
- `data/silver/silver_issues.parquet`
- (Opcional) `data/silver/silver_issues.xlsx`

---

### 🥇 Gold – Regras de Negócio e Indicadores de SLA
- Filtragem de chamados finalizados;
- Cálculo do tempo de resolução em **dias úteis**;
- Exclusão de finais de semana e feriados nacionais do Brasil;
- Definição de SLA esperado conforme prioridade;
- Classificação do SLA como **atendido** ou **violado**;
- Geração de relatórios gerenciais.

**Saídas:**
- `data/gold/gold_sla_issues.parquet`
- `data/gold/gold_sla_issues.xlsx`
- Relatórios agregados em Excel.

---

## Lógica de Cálculo do SLA

As regras de SLA seguem os critérios abaixo:

| Prioridade | SLA Esperado |
|-----------|--------------|
| High      | 24 horas     |
| Medium    | 72 horas     |
| Low       | 120 horas    |

Regras aplicadas:
- Apenas **dias úteis** são considerados;
- Cada dia útil equivale a **24 horas**;
- Finais de semana são excluídos;
- Feriados nacionais do Brasil são obtidos automaticamente via API pública;
- Um chamado está **dentro do SLA** quando o tempo de resolução é menor ou igual ao SLA esperado.

---

## Dicionário de Dados – Tabela Final (Gold)

### `gold_sla_issues`

| Coluna | Descrição |
|------|-----------|
| issue_id | Identificador único do chamado |
| issue_type | Tipo do chamado |
| status | Status final do chamado |
| priority | Prioridade do chamado |
| assignee_id | ID do analista responsável |
| assignee_name | Nome do analista responsável |
| assignee_email | E-mail do analista |
| created_at | Data/hora de criação do chamado |
| resolved_at | Data/hora de resolução |
| resolution_hours | Tempo de resolução em horas úteis |
| sla_expected_hours | SLA esperado conforme prioridade |
| is_sla_met | Indica se o SLA foi atendido ou violado |

---

## Dicionário de Dados – Relatórios

### SLA por Analista
- assignee_name
- issue_count
- avg_resolution_hours

### SLA por Tipo de Chamado
- issue_type
- issue_count
- avg_resolution_hours

### Distribuição de SLA
- is_sla_met (atendido / violado)
- issue_count
- percentage

---

## Estrutura de Pastas

```text
project-root/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── sla_calculation.py
├── resources/
├── logs/
├── run_pipeline.py
├── requirements.txt
└── README.md
```

---

## Instruções para Execução

Pré-requisitos:
- Python 3.10 ou superior

Execução:
```bash
python run_pipeline.py
```

O script:
- Instala automaticamente as dependências;
- Executa todas as camadas do pipeline;
- Gera logs e arquivos finais.

---

## Logs e Monitoramento

- `logs/pipeline.log`: resumo de cada execução;
- `logs/ingest.log`: detalhes da ingestão.

---

## Valor para o Negócio

Este pipeline permite:
- Monitorar cumprimento de SLA;
- Avaliar desempenho operacional;
- Identificar gargalos;
- Apoiar decisões gerenciais baseadas em dados.

---

## Conclusão

Este projeto representa um pipeline de dados completo, robusto e alinhado às melhores práticas de mercado, pronto para uso corporativo e evolução futura.
