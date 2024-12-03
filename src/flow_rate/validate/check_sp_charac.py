import re
import pandas as pd

def check_sp_charac(df_edd_station, df_edd_results):
    """
    Verifica se há acentos ou o caractere 'ç' nas colunas 'comment' dos DataFrames df_edd_results e df_edd_station.
    Se encontrados, marca a linha como erro e adiciona um comentário na coluna 'comentario_correcao'.
    Args:
        df_edd_results (DataFrame): DataFrame contendo a coluna 'comment' a ser verificada.
        df_edd_station (DataFrame): DataFrame contendo a coluna 'comment' a ser verificada.
    Returns:
        None: Atualiza diretamente os DataFrames com as verificações feitas.
    """
    try:
        # Regex para detectar acentos e 'ç'
        regex_acentos = r'[áéíóúâêîôûãõçÁÉÍÓÚÂÊÎÔÛÃÕÇ]'

        # Verificação para df_edd_results
        for index, row in df_edd_results.iterrows():
            comment = row['comment']
            if pd.notna(comment) and re.search(regex_acentos, comment):
                acao_atual = df_edd_results.at[index, 'acao']
                if 'erro' not in str(acao_atual):
                    df_edd_results.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) else acao_atual + " / erro"
                df_edd_results.at[index, 'comentario_correcao'] = df_edd_results.loc[[index], 'comentario_correcao'].apply(
                    lambda x: (str(x) + " / " if pd.notna(x) and x != "" else "") + "Caracteres especiais encontrados na coluna 'comment'"
                ).values[0]

        # Verificação para df_edd_station
        for index, row in df_edd_station.iterrows():
            comment = row['comment']
            if pd.notna(comment) and re.search(regex_acentos, comment):
                acao_atual = df_edd_station.at[index, 'acao']
                if 'erro' not in str(acao_atual):
                    df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) else acao_atual + " / erro"
                df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                    lambda x: (str(x) + " / " if pd.notna(x) and x != "" else "") + "Caracteres especiais encontrados na coluna 'comment'"
                ).values[0]

        return df_edd_results, df_edd_station

    except Exception as e:
        raise Exception(f"Erro em check_sp_charac: {e}")
