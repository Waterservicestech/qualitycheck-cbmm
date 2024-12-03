import pandas as pd
from sqlalchemy.sql import text

def check_station(conn, df_edd_station):
    """Verifica se os station_name no DataFrame estão cadastrados na tabela 'station' do banco de dados.
    
    Args:
        conn: Conexão ao banco de dados.
        df_edd_station (DataFrame): DataFrame com os station_name a serem verificados.
        tipoDado (str): Tipo de dado (Água ou Solo) para definir a tabela a ser usada.
    """
    try:
        # Criar uma cópia de station_name para manter os valores originais
        df_edd_station['station_name_original'] = df_edd_station['station_name']
        
        # Forçar todos os valores de station_name para string
        df_edd_station['station_name'] = df_edd_station['station_name'].astype(str)

        with conn.connect() as connection:
            # Iterar pelas linhas do df_edd_station para verificar cada station_name no banco de dados
            for index, row in df_edd_station.iterrows():
                station_name = row['station_name']

                # Verificar se station_name é um valor válido (não é NaN ou nulo)
                if pd.isna(station_name) or station_name.strip() == '' or station_name.lower() == 'nan':
                    # Se o valor é inválido, marcar como erro e continuar
                    acao_atual = df_edd_station.loc[index, 'acao']
                    if 'erro' not in str(acao_atual):
                        df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                    df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                        lambda x: (str(x) + " / " if pd.notna(x) and x != "" else "") + f"station_name ausente ou inválido."
                    ).values[0]
                    continue

                query = text("""
                    SELECT 1 
                    FROM dbo.station s 
                    WHERE s.Name = :station_name
                """)
                
                result = connection.execute(query, {'station_name': station_name}).fetchone()

                if result is None:
                    # Verificar se há algo em 'acao' e adicionar " / erro" se já houver texto, mas sem duplicar 'erro'
                    acao_atual = df_edd_station.loc[index, 'acao']
                    if 'erro' not in str(acao_atual):
                        df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                    df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                        lambda x: (str(x) + " / " if pd.notna(x) and x != "" else "") + f"station_name {station_name} não está cadastrado no banco."
                    ).values[0]

        # Restaurar os valores originais de station_name
        df_edd_station['station_name'] = df_edd_station['station_name_original']
        df_edd_station.drop(columns=['station_name_original'], inplace=True)

        return df_edd_station

    except Exception as e:
        raise Exception(f"Erro em check_station: {e}")