# 📊 Pipeline de Dados Financeiros Interativo
 
Pipeline de dados batch com arquitetura medalhão (Bronze → Silver → Gold), orquestrado pelo Apache Airflow e com dashboard interativo em Streamlit.
 
---
 
## 🏗️ Arquitetura
 
```
Bronze  →  Silver  →  Gold  →  Dashboard
 CSV         Delta       Delta     Streamlit
```
 
Os dados simulados são gerados para 5 domínios: **vendas**, **frete**, **impostos**, **estoque** e **devoluções**, cobrindo **24 filiais** distribuídas pelo Brasil.
 
---
 
## 📁 Estrutura do Projeto
 
```
projeto_relatorio_financeiro_interativo/
├── dags/
│   └── dag_build_reports.py       # DAG do Airflow
├── dashboard/
│   └── app.py                     # Dashboard Streamlit
├── scripts/
│   ├── mock_generator.py          # Gerador de dados simulados
│   ├── ingest_bronze_to_silver.py # Ingestão Bronze → Silver
│   ├── create_gold_taxes_table.py
│   ├── create_gold_freight_table.py
│   └── create_gold_product_table.py
```
 
> O datalake é gerado localmente e **não está versionado**. Rode o `mock_generator.py` para criar os dados.
 
---
 
## 🚀 Como rodar
 
### Pré-requisitos
 
- Python 3.12
- Java 17
- Apache Airflow instalado em ambiente separado (`airflow-env`)
### 1. Clonar o repositório
 
```bash
git clone https://github.com/seu_usuario/projeto_relatorio_financeiro_interativo.git
cd projeto_relatorio_financeiro_interativo
```
 
### 2. Criar e ativar o ambiente virtual
 
```bash
python -m venv venv_projeto2
source venv_projeto2/bin/activate
```
 
### 3. Gerar os dados simulados
 
```bash
python scripts/mock_generator.py
```
 
### 4. Rodar o pipeline manualmente
 
```bash
python scripts/ingest_bronze_to_silver.py
python scripts/create_gold_taxes_table.py
python scripts/create_gold_freight_table.py
python scripts/create_gold_product_table.py
```
 
### 5. Abrir o dashboard
 
```bash
streamlit run dashboard/app.py
```
 
---
 
## ⚙️ Orquestração com Airflow
 
Copie a DAG para a pasta do Airflow e inicie os serviços:
 
```bash
cp dags/dag_build_reports.py ~/airflow/dags/
 
source ~/airflow-env/bin/activate
airflow webserver --port 8080 &
airflow scheduler &
```
 
Acesse `http://localhost:8080` e ative a DAG `BUILD_REPORTS`.
 
O pipeline roda automaticamente todo dia às **03:00**.
 
---
 
## 🛠️ Stack
 
| Camada | Tecnologia |
|---|---|
| Processamento | PySpark + Delta Lake |
| Orquestração | Apache Airflow |
| Dashboard | Streamlit + Plotly |
| Geração de dados | Faker |
| Linguagem | Python 3.12 |
