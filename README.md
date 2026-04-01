# ARC Automation & Data Consolidation Pipeline

Automação end-to-end para consulta de objetos no sistema ARC dos Correios, com processamento em lote, tolerância a falhas e consolidação de dados em escala.

---

## Arquitetura da solução

A solução é dividida em dois módulos independentes:

### 1. Coleta de dados (Web Automation)

Responsável por:

- Autenticação no sistema ARC  
- Leitura de códigos a partir de Excel  
- Processamento em blocos  
- Sincronização com estado da aplicação  
- Exportação automática dos resultados  

Arquivo: script de automação (Selenium)

---

### 2. Processamento e consolidação

Responsável por:

- Extração de arquivos `.zip`  
- Normalização de CSVs inconsistentes  
- Detecção automática de encoding e delimitador  
- Consolidação em dataset único  

Arquivo: script de consolidação (pandas)

---

## Principais capacidades

### Execução resiliente

- Checkpoint baseado em hash do dataset  
- Retomada automática após falhas  
- Retry progressivo por bloco  

### Sincronização com interface

- Espera baseada em:
  - Badge de resultados  
  - Texto de paginação ("Resultados: X de N")  
- Garantia de consistência antes da exportação  

### Processamento em escala

- Lotes configuráveis (`BLOCK_SIZE`)  
- Suporte a milhares de registros  
- Controle de timeout parametrizável  

### Consolidação robusta

- Leitura tolerante a erros estruturais  
- Reagrupamento de colunas corrompidas  
- Padronização de saída  

---

## Requisitos

- Python 3.9+
- Microsoft Edge instalado
- Edge WebDriver compatível
- Conta no sistema ARC (Correios)

---

## Instalação

```bash
pip install selenium pandas python-dotenv openpyxl
