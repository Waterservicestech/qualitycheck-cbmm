import pandas as pd
from sqlalchemy.sql import text

def check_duplicate(conn, df_edd_station, tipoDado):
    """Verifica se há amostras duplicadas no banco de dados considerando sample_id e station_name.

    Args:
        conn: Conexão ao banco de dados.
        df_edd_station (DataFrame): DataFrame com os station_name e sample_id a serem verificados.
        tipoDado (str): Tipo de dado (Agua ou Solo) para definir a tabela a ser usada.

    Returns:
        None: Atualiza diretamente o df_edd_station com as duplicatas encontradas.
    """
    try:
        # Definir a tabela e a coluna de sample_id com base no tipo de dado (Água ou Solo)
        if tipoDado.lower() == 'agua':
            table_name = 'Monitoring_Sample'
            sample_column = 'Sample_Id'  # Para água, é Sample_Id no banco
        elif tipoDado.lower() == 'solo':
            table_name = 'Soil_Sample'
            sample_column = 'sample_id'  # Para solo, é sample_id em minúsculas no banco
        else:
            raise ValueError(f"TipoDado inválido: {tipoDado}")

        # Iterar pelas linhas do df_edd_station para verificar duplicatas no banco
        with conn.connect() as connection:
            for index, row in df_edd_station.iterrows():
                station_name = row['station_name']
                sample_id = str(row['sample_id'])

                # Atualize a consulta para refletir o tipo correto
                query = text(f"""
                    SELECT s.Name, ms.Sample_Id
                    FROM dbo.{table_name} ms
                    JOIN dbo.station s ON s.ID = ms.Station
                    WHERE ms.Sample_Id = :sample_id
                """)

                # Execute a consulta com o sample_id como string
                result = connection.execute(query, {'sample_id': sample_id}).fetchone()
                
                if result is not None:
                    # Verificar se existe uma string em 'acao' ou inicializar como string vazia
                    acao_atual = df_edd_station.at[index, 'acao']
                    if pd.isna(acao_atual) or acao_atual == "":
                        acao_atual = ""

                    # Atualizar o 'acao' apenas se 'erro' ainda não estiver presente
                    if result[0] == station_name:
                        # Se a amostra já estiver cadastrada para o mesmo ponto
                        if 'erro' not in str(acao_atual):
                            df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                        df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Amostra '{sample_id}' já cadastrada no ponto '{result[0]}' (mesmo ponto) no banco."
                        ).values[0]
                    else:
                        # Caso o ponto seja diferente
                        if 'erro' not in str(acao_atual):
                            df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                        df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Amostra '{sample_id}' já cadastrada no ponto '{result[0]}' (diferente) no banco."
                        ).values[0]

        return df_edd_station

    except Exception as e:
        raise Exception(f"Erro em check_duplicate: {e}")
