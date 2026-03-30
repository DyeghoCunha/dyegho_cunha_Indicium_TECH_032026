#  Northwind Traders Analytics | Modern Data Stack

![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-success?style=for-the-badge)

Este repositório contém o pipeline de Engenharia de Dados e a Modelagem Dimensional desenvolvidos para o desafio técnico da **Indicium**. O projeto transforma a base transacional legado da *Northwind Traders* em um Data Warehouse moderno, escalável e orientado a negócios (Single Source of Truth).

---

##  O Desafio e a Solução
A Northwind Traders sofria com silos de dados e métricas divergentes entre departamentos (Faturamento do CRM vs. ERP). 
A solução foi implementar uma **Arquitetura Medallion** utilizando Databricks e dbt para unificar as regras de negócio complexas (RFM, Market Basket, Cohort, LTV) no back-end, entregando dados limpos, auditáveis e ultra-rápidos para um Dashboard Executivo no Power BI.

##  Arquitetura de Dados (Medallion)

O fluxo de dados foi desenhado para garantir qualidade (Data Contracts) e performance (FinOps):

*  **Bronze (Raw):** Ingestão bruta dos dados do ERP mantendo o histórico inalterado (`views` para otimização de storage inicial).
*  **Silver (Cleansing & Prep):** Padronização de strings, tratamento de nulos, desnormalização leve e geração de **Surrogate Keys (SK)** via hash MD5 para isolar o DW do sistema legado. Armazenamento em Delta Tables.
*  **Gold (Data Marts):** Construção de **OBTs (One Big Tables)** agregadas e modelos de Advanced Analytics, prontos para consumo do VertiPaq Engine do Power BI.

---

##  Principais Decisões de Engenharia

1. **Liquid Clustering vs. Z-Order:** Otimizamos fisicamente as tabelas Fato e Gold utilizando *Liquid Clustering* no Databricks. Isso evita o problema de arquivos pequenos e reorganiza os dados dinamicamente, garantindo varreduras (Data Skipping) em milissegundos sem reescritas caras no cluster.
2. **One Big Table (OBT) para o BI:** Em vez de forçar o Power BI a processar múltiplos JOINs complexos (Snowflake schema) em tempo de execução, transferimos o processamento pesado para a nuvem. Entregamos tabelas flat (`mart_customer_360`, `mart_product_360`) para máxima performance no dashboard.
3. **Data Quality & Contracts:** Nenhuma tabela avança para a camada Gold sem passar por testes de unicidade (`unique`) e não-nulidade (`not_null`) configurados no `schema.yml` do dbt, garantindo integridade referencial absoluta.

---

##  Estrutura do Repositório

```text
 northwind-analytics
 ┣  models
 ┃ ┣  1_bronze          # Views raw (Ingestão)
 ┃ ┣  2_silver          # Limpeza, SKs e Star Schema
 ┃ ┗  3_gold            # Data Marts, RFM, Cohort, Market Basket
 ┣  macros              # Macros customizadas dbt
 ┣  tests               # Testes singulares (Data Quality)
 ┣  target              # dbt docs (HTML gerado)
 ┣  dbt_project.yml     # Configurações raiz do projeto dbt
 ┣  packages.yml        # Dependências (ex: dbt_utils)
 ┗  README.md
```

---

##  Como Executar o Pipeline (dbt CLI)

Para reproduzir este projeto localmente conectado ao seu workspace do Databricks:

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/DyeghoCunha/dyegho_cunha_Indicium_TECH_032026.git
   cd northwind-analytics
   ```

2. **Instale as dependências:**
   ```bash
   dbt deps
   ```

3. **Execute os testes de qualidade (Data Contracts):**
   ```bash
   dbt test
   ```

4. **Execute o pipeline completo (Build):**
   ```bash
   dbt build
   ```

---

##  Documentação Automática (dbt docs)

Toda a linhagem de dados (DAG), dicionário de dados e metadados das tabelas estão documentados no portal interativo do dbt. Para visualizar:

```bash
dbt docs generate
dbt docs serve
```
*(Acesse `http://localhost:8080` no seu navegador)*

---

###  Contato & Autor
**[Dyegho Moraes Costa Gama Cunha]**
Engenheiro de Dados & Analytics
* [LinkedIn](https://linkedin.com/in/dyeghocunha)
* [Portfólio/GitHub](https://github.com/dyeghocunha)

*Desafio técnico desenvolvido em Março/2026. Entregue com muita dedicação (e muito café)! *
