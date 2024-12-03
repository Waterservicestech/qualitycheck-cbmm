import pandas as pd
from datetime import datetime

def check_date(df_edd_station, df_edd_results):
    """
    Verifica os campos analysis_date, analysis_time no df_edd_results e os campos sample_date, sample_time no df_edd_station
    para garantir que seguem os formatos corretos usando datetime. Adiciona comentários de erro se houver problemas de formato,
    e se a data for futura em relação ao dia atual.
    """
    try:
        # Obter a data e hora atuais
        data_atual = datetime.now()

        # Função auxiliar para verificar formato de data e se é futura
        def date_verify_intern(data_str, formatos, campo_nome, index, df, tipo='data'):
            data_str = str(data_str)

            # Se for datetime completo, dividir entre data e hora e verificar apenas a parte relevante
            if ' ' in data_str:
                if tipo == 'data':
                    data_str = data_str.split(' ')[0]  # Extrair a parte da data
                elif tipo == 'hora':
                    data_str = data_str.split(' ')[1]  # Extrair a parte da hora

            for formato in formatos:
                try:
                    data_convertida = datetime.strptime(data_str, formato)
                    # Verificar se a data é futura (apenas se for tipo 'data')
                    if tipo == 'data' and data_convertida > data_atual:
                        acao_atual = df.at[index, 'acao']
                        if 'erro' not in str(acao_atual):
                            df.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) else acao_atual + " / erro"
                        df.at[index, 'comentario_correcao'] = df.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Data futura em {campo_nome}: {data_str}"
                        ).values[0]
                    return  # Se o formato foi válido, sair da função
                except ValueError:
                    continue

            # Se nenhum formato for válido, adicionar erro sem alterar o valor da coluna
            acao_atual = df.at[index, 'acao']
            if 'erro' not in str(acao_atual):
                df.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) else acao_atual + " / erro"
            df.at[index, 'comentario_correcao'] = df.loc[[index], 'comentario_correcao'].apply(
                lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Formato inválido em {campo_nome}: {data_str}"
            ).values[0]

        # Verificar df_edd_results para analysis_date e analysis_time
        for index, row in df_edd_results.iterrows():
            analysis_date = row['analysis_date']
            analysis_time = row['analysis_time']

            # Verificar formato de analysis_date (dd/mm/yyyy ou yyyy-mm-dd)
            date_verify_intern(analysis_date, ["%d/%m/%Y", "%Y-%m-%d"], 'analysis_date', index, df_edd_results, tipo='data')

            # Verificar formato de analysis_time (aceitar HH:MM ou HH:MM:SS)
            date_verify_intern(analysis_time, ["%H:%M", "%H:%M:%S"], 'analysis_time', index, df_edd_results, tipo='hora')

        # Verificar df_edd_station para sample_date e sample_time
        for index, row in df_edd_station.iterrows():
            sample_date = row['sample_date']
            sample_time = row['sample_time']

            # Verificar o formato da data (dd/mm/yyyy ou yyyy-mm-dd)
            date_verify_intern(sample_date, ["%d/%m/%Y", "%Y-%m-%d"], 'sample_date', index, df_edd_station, tipo='data')

            # Verificar o formato de sample_time (aceitar HH:MM ou HH:MM:SS)
            date_verify_intern(sample_time, ["%H:%M", "%H:%M:%S"], 'sample_time', index, df_edd_station, tipo='hora')

        # Após todas as verificações, formatar novamente as colunas para garantir que apenas a data e a hora permaneçam
        df_edd_results['analysis_date'] = df_edd_results['analysis_date'].apply(
            lambda x: x if pd.isna(pd.to_datetime(x, errors='coerce')) else pd.to_datetime(x).strftime('%d/%m/%Y')
        )
        df_edd_station['sample_date'] = df_edd_station['sample_date'].apply(
            lambda x: x if pd.isna(pd.to_datetime(x, errors='coerce')) else pd.to_datetime(x).strftime('%d/%m/%Y')
        )

        # Formatar a hora separadamente, mantendo segundos se presentes
        df_edd_station['sample_time'] = df_edd_station['sample_time'].apply(
            lambda x: x if pd.isna(pd.to_datetime(x, errors='coerce')) else pd.to_datetime(x).strftime('%H:%M:%S')
        )
        df_edd_results['analysis_time'] = df_edd_results['analysis_time'].apply(
            lambda x: x if pd.isna(pd.to_datetime(x, errors='coerce')) else pd.to_datetime(x).strftime('%H:%M:%S')
        )

        return df_edd_results, df_edd_station

    except Exception as e:
        raise Exception(f"Erro em check_date: {e}")