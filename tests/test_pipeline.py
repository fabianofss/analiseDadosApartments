import pytest
import pandas as pd
from src import pipeline

# Teste de Unidade para a função de limpeza
def test_clean_data():
    """Testa a função de limpeza de dados."""
    raw_data = {
        'id': [1, 2, 2, 3],
        'price': ['1,000', '500', '500', None],
        'square_feet': ['800', 'null', '800', '650'],
        'bedrooms': [2, 1, 1, 'null'],
        'bathrooms': [1, 'null', 'null', 1.0]
    }
    df_raw = pd.DataFrame(raw_data)
    
    df_cleaned = pipeline.clean_data(df_raw)
    
    # CORREÇÃO: Apenas uma linha deve sobreviver à limpeza (id=1).
    assert df_cleaned.shape[0] == 1
    assert not df_cleaned.isnull().values.any() # Garante que não há mais NaNs
    assert pd.api.types.is_integer_dtype(df_cleaned['bedrooms'])
    assert pd.api.types.is_integer_dtype(df_cleaned['bathrooms'])
    assert df_cleaned.iloc[0]['id'] == 1 # Verifica se a linha correta sobreviveu

# Teste de Integração (não precisa de mudanças, mas se beneficia das correções no pipeline.py)
def test_pipeline_integration(tmp_path):
    """Testa o fluxo completo do pipeline em um conjunto de dados de amostra."""
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    sample_csv = raw_dir / "sample_data.csv"
    
    sample_data = (
        "id;category;price;bedrooms;bathrooms;square_feet;state;time\n"
        "1;cat1;1200;2;1;900;CA;1577359415\n"
        "2;cat2;800;1;null;600;TX;1577017063\n"
        "3;cat3;2500;3;2;1500;CA;1577359410"
    )
    sample_csv.write_text(sample_data, encoding='iso-8859-1')

    processed_path = tmp_path / "data" / "processed" / "output.parquet"
    figures_path = tmp_path / "reports" / "figuras"
    
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(pipeline, 'DATA_RAW_PATH', sample_csv)
    monkeypatch.setattr(pipeline, 'DATA_PROCESSED_PATH', processed_path)
    monkeypatch.setattr(pipeline, 'FIGURES_PATH', figures_path)

    pipeline.main()

    assert processed_path.exists()
    assert figures_path.exists()
    assert (figures_path / "preco_por_quartos.png").exists()
    assert (figures_path / "preco_mediano_por_estado.png").exists()

    df_processed = pd.read_parquet(processed_path)
    assert not df_processed.empty
    assert 'price_per_sqft' in df_processed.columns
    assert 'post_date' in df_processed.columns