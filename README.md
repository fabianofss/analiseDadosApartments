# Pipeline de Análise de Dados de Aluguéis
Este projeto implementa um pipeline de dados em Python para limpar, processar e analisar um conjunto de dados sobre aluguéis de apartamentos.

## 1. Objetivo
O objetivo é transformar dados brutos e heterogêneos de forma auditável e reprodutível.

**Pergunta Orientadora:**
*Como o número de quartos e a localização (estado) se relacionam com o preço e a área (em pés quadrados) dos apartamentos para aluguel nos EUA?*

## 2. Fonte dos Dados
- **Arquivo:** `data/raw/apartments_for_rent_classified_10K.csv`
- **Origem:** Dataset público sobre aluguéis.

## 3. Passo a Passo de Reprodução

**Pré-requisitos:** Python 3.10+ instalado.

1.  **Clone o repositório:**
    ```bash
    git clone fabianofss/analiseDadosApartments
    ```

2.  **Crie e ative o ambiente virtual:**
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # macOS/Linux
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute os testes para validar o ambiente :**
    ```bash
    pytest
    ```

5.  **Execute o pipeline completo com um único comando:**
    ```bash
    python src/pipeline.py
    ```

## 4. Descrição dos Artefatos Gerados

-   `data/processed/apartments_processed.parquet`: Tabela de dados limpa e processada, pronta para análise ou modelagem.
-   `reports/figuras/preco_por_quartos.png`: Gráfico de boxplot mostrando a distribuição de preços por número de quartos.
-   `reports/figuras/preco_mediano_por_estado.png`: Gráfico de barras com o preço mediano de aluguel para os 15 estados com mais anúncios.
-   `reports/relatorio.md`: Relatório técnico detalhando as decisões, análises e conclusões.