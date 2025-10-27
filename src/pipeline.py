import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import logging
import pathlib

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definição de caminhos
BASE_DIR = pathlib.Path(__file__).parent.parent
DATA_RAW_PATH = BASE_DIR / "data" / "raw" / "apartments_for_rent_classified_10K.csv"
DATA_PROCESSED_PATH = BASE_DIR / "data" / "processed" / "apartments_processed.parquet"
FIGURES_PATH = BASE_DIR / "reports" / "figuras"

# O mkdir irá criar os diretórios se não existirem
DATA_PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
FIGURES_PATH.mkdir(parents=True, exist_ok=True)

# Carregamento dos dados brutos
def load_data(path: pathlib.Path) -> pd.DataFrame:
    """Carrega os dados brutos a partir de um arquivo CSV."""
    logging.info(f"Carregando dados de {path}...")
    try:
        # O dataset deve usar ';' como separador, se não for, ajustar conforme necessário.
        df = pd.read_csv(path, sep=';', encoding='iso-8859-1')
        logging.info("Dados carregados com sucesso.")
        return df
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado em {path}. Verifique o caminho.")
        raise
    except Exception as e:
        logging.error(f"Erro ao carregar os dados: {e}")
        raise

# Limpeza e tratamento dos dados
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza os dados."""
    logging.info("Iniciando a limpeza dos dados...")
    
    # Remover duplicatas baseadas no ID
    initial_rows = len(df)
    df.drop_duplicates(subset=['id'], inplace=True)
    rows_after_id_dedup = len(df)
    logging.info(f"{initial_rows - len(df)} linhas duplicadas removidas.")

    # Remover duplicatas de endereço (anúncios diferentes para o mesmo local) antendo o primeiro que aparecer.
    # Tratar endereços nulos antes, senão eles podem ser todos removidos como duplicatas um do outro.
    df_with_address = df[df['address'].notna()]
    df_no_address = df[df['address'].isna()]
    
    df_with_address = df_with_address.drop_duplicates(subset=['address'], keep='first')
    
    # Recombinar os dados
    df = pd.concat([df_with_address, df_no_address], ignore_index=True)
    
    logging.info(f"{rows_after_id_dedup - len(df)} linhas duplicadas por Endereço removidas.")

    # Converter colunas numéricas, tratando erros.
    # A string 'null' aparece nos dados. Vamos substituir por NaN.
    # O preço tem '$' e ','. Tem que remover.
    #if 'price' in df.columns:
    #    df['price'] = df['price'].astype(str).str.replace(',', '', regex=False)
    #    df['price'] = df['price'].astype(str).str.replace('$', '', regex=False)
    #    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    #if 'square_feet' in df.columns:
    #    df['square_feet'] = df['square_feet'].astype(str).str.replace(',', '', regex=False)
    #    df['square_feet'] = pd.to_numeric(df['square_feet'], errors='coerce')
    cols_to_numeric = ['price', 'square_feet', 'bathrooms', 'bedrooms']
    for col in cols_to_numeric:
        if col in df.columns:
            # Substituir strings comuns que representam nulos
            df[col] = df[col].replace(['null', 'None'], np.nan)
            
            # Limpar caracteres não numéricos (para 'price' e 'square_feet')
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(r'[$,]', '', regex=True)
            
            # Converter para numérico, forçando erros para NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Tratar `bathrooms` e `bedrooms`. A amostra mostra 'null' e números.
    if 'bathrooms' in df.columns:
        df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce')
        df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce')

    # Preencher valores ausentes em colunas numéricas importantes.
    
    if 'bathrooms' in df.columns:
        # Para banheiros quando não houver dado, informar 1 como padrão.
        df['bathrooms'] = df['bathrooms'].fillna(1)
        # Para quartos, usar a mediana para evitar distorções.
        df['bedrooms'] = df['bedrooms'].fillna(df['bedrooms'].median())

    # Remover linhas onde o preço ou a área são nulos, pois são cruciais para a análise.
    # Informar valor padrão ou mediana pode distorcer a análise.
    if 'price' in df.columns and 'square_feet' in df.columns:
        df.dropna(subset=['price', 'square_feet'], inplace=True)

    # Converter para tipos inteiros
    if 'bathrooms' in df.columns:
        df['bathrooms'] = df['bathrooms'].astype(int)
        df['bedrooms'] = df['bedrooms'].astype(int)

    # Remover linhas onde colunas críticas para a análise são nulas
    df.dropna(subset=['price', 'square_feet'], inplace=True)

    if 'has_photo' in df.columns:
        # na coluna has_photo, está informado yes, no e thumbnail, vamos padronizar thumbnail para yes
        df['has_photo'] = df['has_photo'].replace('Thumbnail', 'yes')
        # uma vez que temos todos campos informados como yes e no pode ser alterado para tipo numérico
        df['has_photo'] = df['has_photo'].map({'yes': True, 'no': False})
    
    # Tratar pets_allowed
    if 'pets_allowed' in df.columns:
        df['pets_allowed'] = df['pets_allowed'].replace('None', 'no')
        df['pets_allowed'] = df['pets_allowed'].replace('null', 'no')
        df['pets_allowed'] = df['pets_allowed'].replace('Cats,Dogs', 'yes')
        df['pets_allowed'] = df['pets_allowed'].replace('Cats', 'yes')
        df['pets_allowed'] = df['pets_allowed'].replace('Dogs', 'yes')
        df['pets_allowed'] = df['pets_allowed'].map({'yes': True, 'no': False})

    logging.info("Limpeza de dados concluída.")
    return df

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Cria novas variáveis para a análise."""
    logging.info("Iniciando engenharia de variáveis...")
    
    # Criar preço por pé quadrado para normalizar a comparação de preços
    # Evitar divisão por zero
    if 'square_feet' in df.columns and 'price' in df.columns:
        df = df[df['square_feet'] > 0].copy()
        df['price_per_sqft'] = df['price'] / df['square_feet']

    # Converter o timestamp `time` para um formato de data legível.
    if 'time' in df.columns:
        df['post_date'] = pd.to_datetime(df['time'], unit='s')

    # Agrupar sources menores que 1% de aptos como 'Outras' para apresntar no gráfico
    # Contar a quantidade de anúncios por source
    if 'source' in df.columns:
        source_counts = df['source'].value_counts()
        total_entries = len(df)
        pec_minimo = 0.01 * total_entries  # manor que 1% do total
        df['source_grouped'] = df['source'].apply(lambda x: x if source_counts[x] >= pec_minimo else 'Outras')
        # Criar legenda do source com o percentual
        df_source_grouped = df['source_grouped'].value_counts(normalize=True) * 100
        df['source_grouped'] = df['source_grouped'].apply(lambda x: f"{x} ({df_source_grouped[x]:.2f}%)")

    # Criar bins de 500 sqft e calcular a média de preço por faixa para gerar tendência
    if 'square_feet' in df.columns:
        df['square_feet_bin'] = (df['square_feet'] // 500) * 500
        df_price_trend = df.groupby('square_feet_bin', as_index=False)['price'].mean()
    
    logging.info("Engenharia de variáveis concluída.")
    return df

# Analisar e apresentação gráfica
def analyze_and_visualize(df: pd.DataFrame):
    """Realiza a análise exploratória e gera os gráficos."""
    logging.info("Iniciando análise exploratória e geração de gráficos...")
    
    # Gráfico 1: Relação entre Preço e Número de Quartos (Boxplot)
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='bedrooms', y='price')
    plt.title('Distribuição de Preço por Número de Quartos')
    plt.xlabel('Número de Quartos')
    plt.ylabel('Preço (USD)')
    plt.ylim(0, 10000)  # DECISÃO: Limitar o eixo Y para melhor visualização, removendo outliers extremos.
    plt.savefig(FIGURES_PATH / "preco_por_quartos.png")
    logging.info(f"Gráfico 'preco_por_quartos.png' salvo em {FIGURES_PATH}")

    # Gráfico 2: Preço médio por Estado (Barplot)
    # Focar nos 15 estados com mais anúncios para um gráfico mais limpo.
    top_states = df['state'].value_counts().nlargest(15).index
    df_top_states = df[df['state'].isin(top_states)]
    
    plt.figure(figsize=(12, 8))
    state_price_order = df_top_states.groupby('state')['price'].median().sort_values(ascending=False).index
    sns.barplot(data=df_top_states, x='state', y='price', order=state_price_order, estimator=np.median)
    plt.title('Preço Mediano de Aluguel por Estado (Top 15)')
    plt.xlabel('Estado')
    plt.ylabel('Preço Mediano (USD)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(FIGURES_PATH / "preco_mediano_por_estado.png")
    logging.info(f"Gráfico 'preco_mediano_por_estado.png' salvo em {FIGURES_PATH}")
    
    plt.close('all') # Fecha todas as figuras para liberar memória

# Salvar dados processados
def save_processed_data(df: pd.DataFrame, path: pathlib.Path):
    """Salva os dados processados em formato Parquet."""
    logging.info(f"Salvando dados processados em {path}...")
    try:
        df.to_parquet(path, index=False)
        logging.info("Dados processados salvos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar os dados: {e}")
        raise

def main():
    """Orquestra a execução do pipeline completo."""
    logging.info("="*50)
    logging.info("INICIANDO O PIPELINE DE ANÁLISE DE DADOS")
    logging.info("="*50)
    
    # Etapa 1: Carregar
    raw_df = load_data(DATA_RAW_PATH)
    
    # Etapa 2: Limpar
    cleaned_df = clean_data(raw_df.copy()) # Usar .copy() para evitar SettingWithCopyWarning
    
    # Etapa 3: Engenharia de Variáveis
    featured_df = feature_engineering(cleaned_df.copy())
    
    # Etapa 4: Análise e Visualização
    analyze_and_visualize(featured_df)
    
    # Etapa 5: Salvar dados processados
    save_processed_data(featured_df, DATA_PROCESSED_PATH)
    
    logging.info("="*50)
    logging.info("PIPELINE EXECUTADO COM SUCESSO")
    logging.info("="*50)

if __name__ == "__main__":
    main()