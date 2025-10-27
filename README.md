# Pipeline de Análise de Dados de Aluguéis
Este projeto implementa um pipeline de dados em Python para limpar, processar e analisar um conjunto de dados sobre aluguéis de apartamentos.

## 1. Objetivo & Pergunta Orientadora
O objetivo é transformar dados brutos e heterogêneos de forma auditável e reprodutível.

**Pergunta Orientadora:**
*Como o número de quartos e a localização (estado) se relacionam com o preço e a área (em pés quadrados) dos apartamentos para aluguel nos EUA?*
**Pergunta Orientadora:** *Como o número de quartos e a localização (estado) se relacionam com o preço e a área (em pés quadrados) dos apartamentos para aluguel nos EUA?*

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

## 4. Tratamento e limpeza dos dados aplicados
    Para limeza dos dados foram removidos registros duplicados tendo o ID e o endereço como critério
    Na etapa de tratamento aplicadas as seguintes estratégias
        Foram removidas colunas com muita informação sem inportancia para as analises como descrições ou observações
        Colnas de valores foram padronizadas, removendo caracteres e convertendo para tipo numerico
        Colunas de quantidades foram padronizadas e em casos adotado a média e em casos especiais como badroom foi adotado 1 quando não se tem a informação
        Colunas do tipo lógica se adotou False para registros sem a informação
        Informações padronizadas como no caso da coluna "pets_allowed" que em alguns casos especificava o animal permitido, alteramos para True/False
        Criamos um campo calculado para obter o valor do metro quadrado afim de facilitar analise e processamento

## 5. Descrição dos Artefatos Gerados

-   `data/processed/apartments_processed.parquet`: Tabela de dados limpa e processada, pronta para análise ou modelagem.
-   `reports/figuras/preco_por_quartos.png`: Gráfico de boxplot mostrando a distribuição de preços por número de quartos.
-   `reports/figuras/preco_mediano_por_estado.png`: Gráfico de barras com o preço mediano de aluguel para os 15 estados com mais anúncios.
-   `reports/relatorio.md`: Relatório técnico detalhando as decisões, análises e conclusões.

## 6. Limitações e Próximos Passos

Consulte o `reports/relatorio.md` para uma discussão detalhada sobre as limitações do estudo e sugestões para trabalhos futuros.