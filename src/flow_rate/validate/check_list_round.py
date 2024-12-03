import pandas as pd
def check_list_round(depara_file_path, df_edd_station, tipoDado):
    """
    Verifica se os valores das colunas 'sample_type', 'quality_code', 'laboratory', 'matrix',
    'periodicity', e 'monitoring_round' no df_edd_station estão entre os valores permitidos
    de acordo com a aba 'Lists' do arquivo de De-Para. Valores NaN são ignorados.
    Também preenche automaticamente 'record_resp' com 'EDD Laboratorio' e 'sampler_name' com 'Terceirizada' no df_edd_station.

    Args:
        df_edd_station (DataFrame): DataFrame onde 'record_resp' e 'sampler_name' serão preenchidos e valores verificados.
        depara_file_path (str): Caminho do arquivo De-Para que contém a aba 'Lists'.

    Returns:
        None: Atualiza diretamente o df_edd_station com as verificações e preenchimentos.
    """
    try:
        # Preencher 'record_resp' e 'sampler_name' no df_edd_station
        df_edd_station['record_resp'] = 'EDD Laboratorio'
        df_edd_station['sampler_name'] = 'Terceirizada'

        # Ler a aba 'Lists' do arquivo De-Para
        df_lists = pd.read_excel(depara_file_path, sheet_name='Lists')

        # Dicionários de valores válidos
        if tipoDado.lower() == 'agua':
            valores_validos = {
                'sample_type': df_lists['sample_type'].dropna().tolist(),
                'quality_code': df_lists['quality_code'].dropna().tolist(),
                'laboratory': df_lists['laboratory'].dropna().tolist(),
                'matrix': df_lists['matrix'].dropna().tolist(),
                'periodicity': df_lists['periodicity'].dropna().tolist(),
            }

            # Iterar sobre as colunas para verificar se os valores estão corretos no df_edd_station
            for col in ['sample_type', 'quality_code', 'laboratory', 'matrix', 'periodicity']:
                for index, row in df_edd_station.iterrows():
                    valor = row[col]

                    # Verificar se o valor está vazio ou nulo
                    if pd.isna(valor) or valor == "":
                        acao_atual = df_edd_station.at[index, 'acao']
                        if 'erro' not in str(acao_atual):
                            df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                        df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Campo '{col}' de lista obrigatória está vazio ou nulo."
                        ).values[0]
                        continue

                    # Verificar se o valor está na lista válida
                    if valor not in valores_validos[col]:
                        acao_atual = df_edd_station.at[index, 'acao']
                        if 'erro' not in str(acao_atual):
                            df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                        df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Campo '{col}' de lista obrigatória com valor inválido: '{valor}'"
                        ).values[0]

        elif tipoDado.lower() == 'solo':
            valores_validos = {
                'sample_type': df_lists['sample_type'].dropna().tolist(),
                'laboratory': df_lists['laboratory'].dropna().tolist(),
                'matrix': df_lists['matrix'].dropna().tolist(),
                'periodicity': df_lists['periodicity'].dropna().tolist(),
            }

            for col in ['sample_type', 'laboratory', 'matrix', 'periodicity']:
                for index, row in df_edd_station.iterrows():
                    valor = row[col]

                    if pd.isna(valor) or valor == "":
                        acao_atual = df_edd_station.at[index, 'acao']
                        if 'erro' not in str(acao_atual):
                            df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                        df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Campo '{col}' de lista obrigatória está vazio ou nulo."
                        ).values[0]
                        continue

                    if valor not in valores_validos[col]:
                        acao_atual = df_edd_station.at[index, 'acao']
                        if 'erro' not in str(acao_atual):
                            df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                        df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                            lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Campo '{col}' de lista obrigatória com valor inválido: '{valor}'"
                        ).values[0]

        # Verificação do campo 'monitoring_round'
        for index, row in df_edd_station.iterrows():
            periodicity = row['periodicity'].strip().lower()
            sample_date = pd.to_datetime(row['sample_date'], dayfirst=True)
            year = sample_date.year

            if periodicity == "mensal":
                month = sample_date.month
                expected_monitoring_round = f"{year}M{month}"
            elif periodicity == "trimestral":
                trimestre = (sample_date.month - 1) // 3 + 1
                expected_monitoring_round = f"{year}T{trimestre}"
            else:
                continue  # Ignorar se não for "Mensal" ou "Trimestral"

            # Verificar se o valor atual corresponde ao esperado
            current_monitoring_round = row['monitoring_round']
            if current_monitoring_round != expected_monitoring_round:
                acao_atual = df_edd_station.at[index, 'acao']
                if 'erro' not in str(acao_atual):
                    df_edd_station.at[index, 'acao'] = 'erro' if pd.isna(acao_atual) or acao_atual == "" else acao_atual + " / erro"
                df_edd_station.at[index, 'comentario_correcao'] = df_edd_station.loc[[index], 'comentario_correcao'].apply(
                    lambda x: (str(x) + " / " if pd.notna(x) else "") + f"monitoring_round incorreto: '{current_monitoring_round}', esperado: '{expected_monitoring_round}'"
                ).values[0]

        return df_edd_station

    except Exception as e:
        raise Exception(f"Erro em check_list_round: {e}")