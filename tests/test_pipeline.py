import pytest
import pandas as pd
import numpy as np
from src import pipeline

# Teste de Unidade para a função `clean_data`
def test_clean_data():
    """Testa a função `clean_data` com um DataFrame de amostra."""
    raw_data = {
        'id': [1, 2, 2, 3, 4],
        'address': ['Rua A, 1', 'Rua B, 2', 'Rua B, 2', None, 'Rua A, 1'],
        'price': ['$1,000', '500', '500', '800', '1,000'],
        'square_feet': ['800', 'null', '800', '650', '800'],
        'bedrooms': [2, 1, 1, 'null', 2],
        'bathrooms': [1, 'None', np.nan, 1.0, 1],
        'has_photo': ['Thumbnail', 'no', 'yes', 'yes', 'no'],
        'pets_allowed': ['Cats,Dogs', 'None', 'null', 'Cats', 'Dogs']
    }
    df_raw = pd.DataFrame(raw_data)
    
    df_cleaned = pipeline.clean_data(df_raw.copy())
    
    # Após todas as limpezas (id dup, address dup, sqft null), devem sobrar 2 linhas
    assert len(df_cleaned) == 2, "Deveriam restar 2 linhas após a limpeza."
    assert set(df_cleaned['id']) == {1, 3}, "Os IDs restantes devem ser 1 e 3."
    
    # Verifica conversões de tipo
    assert pd.api.types.is_integer_dtype(df_cleaned['bedrooms'])
    assert pd.api.types.is_bool_dtype(df_cleaned['has_photo'])
    assert pd.api.types.is_bool_dtype(df_cleaned['pets_allowed'])

# Teste de Integração para o pipeline completo
def test_pipeline_integration(tmp_path, monkeypatch):
    """Testa o fluxo completo do pipeline e a criação de todos os artefatos."""
    # 1. PREPARAÇÃO
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    sample_csv_path = raw_dir / "sample_data.csv"

    processed_path = tmp_path / "data" / "processed" / "output.parquet"
    figures_path = tmp_path / "reports" / "figuras"
    
    # *** CORREÇÃO CRÍTICA: CRIAR O DIRETÓRIO DE FIGURAS ***
    figures_path.mkdir(parents=True, exist_ok=True)

    sample_data = (
        "id;address;price;bedrooms;bathrooms;square_feet;state;time;source;has_photo;pets_allowed\n"
        "1;Main St 123;$1,200.50;2;1;900;CA;1577359415;Zumper;yes;Dogs\n"
        "2;Broadway 456;800;1;null;600;TX;1577017063;Apartments.com;no;None\n"
        "3;Sunset Blvd 789;2500;3;2;1500;CA;1577359410;Zumper;Thumbnail;Cats\n"
    )
    sample_csv_path.write_text(sample_data, encoding='utf-8')

    monkeypatch.setattr(pipeline, 'DATA_RAW_PATH', sample_csv_path)
    monkeypatch.setattr(pipeline, 'DATA_PROCESSED_PATH', processed_path)
    monkeypatch.setattr(pipeline, 'FIGURES_PATH', figures_path)

    # 2. AÇÃO
    pipeline.main()

    # 3. VERIFICAÇÃO
    assert processed_path.exists(), "O arquivo Parquet processado não foi criado."
    assert (figures_path / "preco_por_quartos.png").exists(), "O gráfico de preço por quartos não foi criado."

    df_processed = pd.read_parquet(processed_path)
    assert len(df_processed) == 3, "O DataFrame processado deve ter 3 linhas."
    
    # Verifica se a limpeza de preço funcionou
    price_id_1 = df_processed.loc[df_processed['id'] == 1, 'price'].iloc[0]
    assert np.isclose(price_id_1, 1200.50), "A limpeza de preço com '$', ',' e '.' falhou."