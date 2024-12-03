import re
import pandas as pd
def check_result_value(df_edd_results):
    """
    Verifica se há valores negativos ou texto nos campos de resultados numéricos no DataFrame df_edd_results.
    
    Args:
        df_edd_results (DataFrame): DataFrame que contém a coluna 'result_num_hga' para verificação.

    Returns:
        None: Atualiza diretamente o df_edd_results com as verificações feitas.
    """
    try:
        for index, row in df_edd_results.iterrows():
            resultado = row['result_num_hga']
            texto_resultado = row['result_txt_hga']
            acao_atual = df_edd_results.at[index, 'acao'] if pd.notna(df_edd_results.at[index, 'acao']) else ""

            # Se resultado for string, tenta substituir a vírgula e converter para número
            if isinstance(resultado, str):
                # Substituir vírgula por ponto e tentar converter para float
                try:
                    resultado = float(resultado.replace(',', '.'))
                except ValueError:
                    pass

            # Verificar se é um valor numérico e se é negativo
            if pd.api.types.is_numeric_dtype(type(resultado)) and pd.notna(resultado):
                if resultado < 0:
                    # Marcar como erro e adicionar comentário
                    if 'erro' not in acao_atual:
                        df_edd_results.at[index, 'acao'] = acao_atual + " / erro" if acao_atual else "erro"
                    df_edd_results.at[index, 'comentario_correcao'] = df_edd_results.loc[[index], 'comentario_correcao'].apply(
                        lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Valor negativo encontrado em 'result_num_hga': {resultado}"
                    ).values[0]
            else:
                # Verificar se é um texto ou valor não numérico
                if isinstance(resultado, str):
                    if 'erro' not in acao_atual:
                        df_edd_results.at[index, 'acao'] = acao_atual + " / erro" if acao_atual else "erro"
                    df_edd_results.at[index, 'comentario_correcao'] = df_edd_results.loc[[index], 'comentario_correcao'].apply(
                        lambda x: (str(x) + " / " if pd.notna(x) else "") + "Texto encontrado em campo numérico 'result_num_hga'"
                    ).values[0]

            # Verificar se há números no campo de texto 'result_txt_hga'
            if texto_resultado is not None:
                texto_resultado_str = str(texto_resultado)
                if re.search(r'\d', texto_resultado_str):
                    # Se a string contém qualquer número, será marcada como erro
                    if 'erro' not in acao_atual:
                        df_edd_results.at[index, 'acao'] = acao_atual + " / erro" if acao_atual else "erro"
                    df_edd_results.at[index, 'comentario_correcao'] = df_edd_results.loc[[index], 'comentario_correcao'].apply(
                        lambda x: (str(x) + " / " if pd.notna(x) else "") + f"Números encontrados no campo de texto 'result_txt_hga'"
                    ).values[0]

        return df_edd_results

    except Exception as e:
        raise Exception(f"Erro em check_result_value: {e}")
