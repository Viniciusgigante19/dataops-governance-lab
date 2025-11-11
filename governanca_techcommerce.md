# 🧭 Governança de Dados - TechCommerce

## 📘 Visão Geral

Este documento estabelece o modelo de **Governança de Dados da TechCommerce**, com foco na **qualidade, confiabilidade e rastreabilidade** das informações que alimentam os produtos analíticos e operacionais da empresa.

A governança cobre todo o ciclo de vida dos dados — desde a ingestão até a publicação dos relatórios de qualidade — implementado por meio de uma **pipeline modular** composta pelos seguintes módulos:

| Etapa | Script | Responsabilidade Principal |
|-------|---------|----------------------------|
| Ingestão e Validação Inicial | `pipeline_ingestao.py` | Carregar dados brutos de múltiplas fontes (CSV), aplicar validações e registrar logs |
| Análise de Problemas | `analise_problemas.ipynb` | Inspeção exploratória e diagnóstico de anomalias nos datasets |
| Validação Avançada | `great_expectations_setup.py` | Criação de *Expectation Suites* com regras formais de validação e testes automatizados |
| Correção e Padronização | `correcao_automatica.py` | Limpeza, deduplicação, padronização de formatos e consistência entre entidades |
| Enriquecimento de Dados | `enriquecimento_dados.py` | Simulação de geocodificação, categorização e cálculo de métricas derivadas |
| Monitoramento e Relatórios | `dashboard_qualidade.py` | Geração dos *Data Docs* do Great Expectations e relatórios executivos HTML/PDF |
| Diretrizes e Políticas | `governanca_techcommerce.md` | Documento oficial de governança e qualidade de dados (este arquivo) |


---

## 🧩 Organograma de Dados

A gestão dos dados segue a hierarquia de papéis **Data Owner**, **Data Steward** e **Data Custodian**, garantindo clareza de responsabilidade em cada domínio.

| Domínio | Data Owner | Data Steward | Data Custodian |
|----------|-------------|---------------|----------------|
| **Clientes** | Diretoria de CRM e Relacionamento | Analista de Dados de Clientes | Engenheiro de Dados responsável pelo `pipeline_ingestao.py` e `correcao_automatica.py` |
| **Produtos** | Diretoria de Catálogo e Precificação | Especialista de Produto | Engenheiro de Dados responsável pelo `enriquecimento_dados.py` |
| **Vendas** | Diretoria Comercial | Analista de Vendas e BI | Engenheiro de Dados responsável pela integração com `great_expectations_setup.py` |
| **Logística** | Diretoria de Operações | Coordenador de Transporte e Entrega | Engenheiro de Dados responsável pelo tratamento e cruzamento em `pipeline_ingestao.py` |

**Responsabilidades:**

- **Data Owner:** Define regras de negócio, políticas de uso e requisitos de qualidade.  
- **Data Steward:** Supervisiona o cumprimento das políticas e coordena correções e enriquecimentos.  
- **Data Custodian:** Implementa tecnicamente os processos de ingestão, validação, correção e monitoramento.  

---

## 📏 Políticas de Qualidade de Dados

As políticas foram definidas considerando as **dimensões clássicas de qualidade**, adaptadas ao contexto operacional da TechCommerce.

| Dimensão | Definição | Limite Aceitável | Ação Corretiva |
|-----------|------------|------------------|----------------|
| **Completude** | Percentual de campos obrigatórios preenchidos (ex: `id_cliente`, `email`, `id_produto`) | ≥ 98% (máx. 2% incompletos) | Preenchimento via regra de negócio (`correcao_automatica.py`) ou exclusão controlada |
| **Unicidade** | Ausência de duplicatas em chaves primárias e e-mails | 100% | Deduplicação automática no `correcao_automatica.py` |
| **Validade** | Conformidade com formato e domínio (ex: e-mails válidos, UF com 2 letras) | ≥ 99% válidos | Padronização regex e validação via Great Expectations |
| **Consistência** | Coerência entre datasets (FK válidas, relacionamentos corretos) | ≥ 98% consistentes | Correção cruzada automatizada (`correcao_automatica.py`) |
| **Acurácia** | Veracidade dos valores conforme fonte autorizada | Avaliada caso a caso | Revisão manual com Data Owner |
| **Atualidade** | Dados recentes e sincronizados com as fontes operacionais | ≤ 24h de defasagem | Execução diária do `pipeline_ingestao.py` |
| **Rastreabilidade** | Capacidade de auditar origem e transformações | Total | Logs detalhados no `pipeline_ingestao.py` e `dashboard_qualidade.py` |

Cada violação dessas dimensões gera uma **flag de qualidade** registrada no dataset e no relatório executivo (`dashboard_qualidade.py`).

---

## 🔍 Glossário de Negócios

### **Clientes**

- **Cliente Ativo:** indivíduo que realizou ao menos uma compra nos últimos 12 meses.  
  - Determinação: `vendas.data_venda >= hoje - 365 dias`.  
- **Telefone Padrão:** 11 dígitos, numérico, sem formatação.  
  - Correção automática no `correcao_automatica.py`.  
- **E-mail Válido:** formato `^[\w\.-]+@[\w\.-]+\.\w+$`.  
  - Validado no `great_expectations_setup.py`.  
- **Idade do Cliente:** calculada a partir da data de nascimento (`enriquecimento_dados.py`).

### **Produtos**

- **Produto Ativo:** consta no catálogo e possui preço e categoria válidos.  
- **Categoria Automática:** inferida por regras de descrição (`enriquecimento_dados.py`).  
- **Preço Padrão:** armazenado em float, com separador decimal `.`.

### **Vendas**

- **Venda Válida:** transação com cliente, produto e data de venda válidos.  
  - Validada nas foreign keys (`correcao_automatica.py`).  
- **Valor Total:** `quantidade * preco`.  
- **Canal de Venda:** campo padronizado (`online`, `loja_fisica`, `marketplace`).

### **Logística**

- **Entrega Concluída:** status confirmado com data de entrega não nula.  
- **Tempo de Entrega:** diferença entre `data_envio` e `data_entrega`.  
  - Calculado e validado em `enriquecimento_dados.py`.  
- **Região de Destino:** normalizada a partir do CEP (simulação de geocodificação).  

---

## 🧠 Integração entre os Módulos da Pipeline

A pipeline de dados da TechCommerce é composta por **módulos independentes e encadeados**, o que permite rastreabilidade e versionamento em cada etapa.

1. **Ingestão (`pipeline_ingestao.py`)**  
   - Lê dados originais de `/datasets/`.  
   - Aplica schema validation com Pandera ou validações manuais.  
   - Registra logs e salva dados processados em `/data/`.

2. **Análise (`analise_problemas.ipynb`)**  
   - Executa inspeção exploratória de anomalias.  
   - Gera insights sobre problemas de completude, formato e consistência.

3. **Validação (`great_expectations_setup.py`)**  
   - Cria *Expectation Suites* para cada domínio (Clientes, Produtos, Vendas, Logística).  
   - Garante integridade de formato, unicidade e consistência referencial.  
   - Registra resultados em Data Docs.

4. **Correção (`correcao_automatica.py`)**  
   - Corrige duplicatas, campos nulos, e-mails, datas e inconsistências.  
   - Padroniza formatos (telefone, CEP, datas ISO).  
   - Revalida as foreign keys entre tabelas.

5. **Enriquecimento (`enriquecimento_dados.py`)**  
   - Simula geocodificação e categorização automática.  
   - Calcula métricas derivadas (idade, tempo de entrega, ticket médio).  
   - Adiciona colunas auxiliares e flags de qualidade.

6. **Qualidade e Relatórios (`dashboard_qualidade.py`)**  
   - Consolida métricas de todos os datasets.  
   - Gera **relatórios HTML e PDF executivos** com status da qualidade.  
   - Integra *Data Docs* do Great Expectations e métricas customizadas.

---

## 🧾 Padrões de Formato e Relacionamentos

| Campo | Tipo / Formato | Exemplo | Observações |
|--------|----------------|----------|--------------|
| `id_cliente` | Inteiro | 1001 | PK em `clientes.csv`, FK em `vendas.csv` |
| `email` | String (regex) | cliente@email.com | Único, validado e padronizado |
| `telefone` | String (11 dígitos) | 11987654321 | Normalizado, sem traços |
| `data_nascimento` | Data ISO (`YYYY-MM-DD`) | 1990-06-10 | Validação e cálculo de idade |
| `id_produto` | Inteiro | 501 | PK em `produtos.csv`, FK em `vendas.csv` |
| `preco` | Float | 59.90 | Ponto como separador decimal |
| `data_venda` | Data ISO | 2025-10-15 | Necessária para "Venda Válida" |
| `data_envio` / `data_entrega` | Data ISO | 2025-10-17 | Usadas para cálculo de tempo de entrega |

---

## ⚙️ Fluxo Operacional do Pipeline

```text
/datasets (dados brutos)
       │
       ▼
[pipeline_ingestao.py]
       │ → validação e schema
       ▼
[data] (dados tratados)
       │
       ├── [correcao_automatica.py] → limpeza e padronização
       │
       ├── [enriquecimento_dados.py] → enriquecimento e métricas derivadas
       │
       ├── [great_expectations_setup.py] → validações formais e Data Docs
       │
       ▼
[dashboard_qualidade.py] → relatório executivo HTML/PDF
