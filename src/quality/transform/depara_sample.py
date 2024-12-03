import pandas as pd

def depara_sample(df_edd_station, df_depara_sample):
    """
    Verifica e corrige campos relacionados às amostras no DataFrame EDD com base no arquivo De-Para.

    Args:
        df_edd_station (DataFrame): DataFrame contendo os dados de estações no EDD.
        df_depara_sample (DataFrame): DataFrame contendo as regras de De-Para para amostras.

    Returns:
        DataFrame: DataFrame atualizado com as correções aplicadas.
    """
    try:
        if 'comentario_correcao' not in df_edd_station.columns:
            df_edd_station['comentario_correcao'] = ""
        if 'acao' not in df_edd_station.columns:
            df_edd_station['acao'] = ""

        # Normalizar sample_name para case-insensitive (tudo em minúsculas)
        df_edd_station['sample_name'] = df_edd_station['sample_name'].str.lower()
        df_depara_sample['sample_name'] = df_depara_sample['sample_name'].str.lower()

        # Realizar o merge para verificar correspondência dos sample_name
        merged_sample_df = pd.merge(
            df_edd_station[['sample_name', 'station_name', 'matrix']],
            df_depara_sample[['sample_name', 'name', 'Matriz']],
            on='sample_name',
            how='left',
            indicator=True
        )

        def verificar_e_corrigir_sample(row_edd, row_merge):
            acao = None
            comentario = None

            # Verificar e corrigir matrix
            if row_edd['matrix'] != row_merge['Matriz']:
                valor_anterior_matrix = row_edd['matrix']
                df_edd_station.at[row_edd.name, 'matrix'] = row_merge['Matriz']

                comentario_anterior = df_edd_station.at[row_edd.name, 'comentario_correcao']
                comentario_matrix = f"Correção: matrix ajustado de '{valor_anterior_matrix}' para '{row_merge['Matriz']}'"
                comentario = comentario_matrix
                acao = 'corrigido'

                if pd.notna(comentario_anterior) and comentario_anterior != "":
                    comentario = comentario_anterior + " / " + comentario_matrix

            # Verificar e corrigir station_name
            if row_edd['station_name'] != row_merge['name']:
                valor_anterior_station = row_edd['station_name']
                df_edd_station.at[row_edd.name, 'station_name'] = row_merge['name']

                comentario_anterior = df_edd_station.at[row_edd.name, 'comentario_correcao']
                comentario_station = f"Correção: station_name ajustado de '{valor_anterior_station}' para '{row_merge['name']}'"
                comentario = comentario_station
                acao = 'corrigido'

                if pd.notna(comentario_anterior) and comentario_anterior != "":
                    comentario = comentario_anterior + " / " + comentario_station

            return acao, comentario

        # Aplicar a verificação e correção em df_edd_station
        for index, row_edd in df_edd_station.iterrows():
            row_merge = merged_sample_df.iloc[index]
            acao, comentario = verificar_e_corrigir_sample(row_edd, row_merge)
            if acao:
                df_edd_station.at[index, 'acao'] = acao
                df_edd_station.at[index, 'comentario_correcao'] = comentario

        # Restaurar o valor original da coluna sample_name
        df_edd_station['sample_name'] = df_edd_station['sample_name_original']
        df_edd_station.drop(columns=['sample_name_original'], inplace=True)

        return df_edd_station

    except Exception as e:
        print(f"Error verifying samples: {e}")
        raise
