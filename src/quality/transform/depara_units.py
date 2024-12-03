import pandas as pd

def depara_units(df_edd_results, df_depara_units):
    """
    Validates and corrects units in EDD results based on De-Para mappings.

    Args:
        df_edd_results (DataFrame): EDD results data.
        df_depara_units (DataFrame): Mapping of units.

    Returns:
        None: Updates the `df_edd_results` in place.
    """
    try:
        def verificar_e_corrigir_units(row_edd, row_merge, index):
            # Definir as combinações especiais que devem aplicar a conversão
            conversoes = {
                ('µg/l', 'mg/l'): 0.001,
                ('ug/l', 'mg/l'): 0.001,
                ('mg/l', 'ug/l'): 1000,
                ('miligrama-litro', 'ug/l'): 1000
            }
            
            # Verificar se a combinação atual está nas conversões
            original_unit = str(row_edd['unit_org']).lower() if isinstance(row_edd['unit_org'], str) else row_edd['unit_org']
            hga_unit = str(row_edd['unit_hga']).lower() if isinstance(row_edd['unit_hga'], str) else row_edd['unit_hga']

            if (original_unit, hga_unit) in conversoes:
                # Realizar a conversão de valores
                fator = conversoes[(original_unit, hga_unit)]
                
                # Verifica se o valor de result_value existe e aplica a conversão
                result_value = row_edd['result_value']
                
                # Remover os caracteres '<' e '>' e converter o valor de string para número, substituindo a vírgula por ponto
                result_value_numeric = pd.to_numeric(
                    str(result_value).replace('<', '').replace('>', '').replace(',', '.'), errors='coerce')
                
                if pd.notna(result_value_numeric):
                    valor_anterior = result_value_numeric
                    # Aplicar a conversão
                    df_edd_results.at[index, 'result_num_hga'] = valor_anterior * fator
                    
                    # Adicionar o comentário de conversão
                    comentario_anterior = df_edd_results.at[index, 'comentario_correcao']
                    comentario = (f"Conversão de unidade realizada: {original_unit} para {hga_unit}, "
                                f"valor ajustado de {valor_anterior} para {valor_anterior * fator}.")
                    if pd.notna(comentario_anterior) and comentario_anterior != "":
                        comentario = comentario_anterior + " / " + comentario
                    return 'corrigido', comentario
                else:
                    # Caso não seja um valor numérico, reportar o erro
                    return 'erro', 'Valor numérico ausente ou inválido para conversão de unidade.'
            
            # Caso não seja uma das combinações especiais, verificar se precisa corrigir a unidade
            if row_merge['_merge'] == 'left_only':
                return 'erro', 'Não encontrado correspondência no de-para para unit_org'
            elif row_edd['unit_hga'] != row_merge['hga_unit']:
                # Capturar o valor anterior de unit_hga
                valor_anterior = row_edd['unit_hga']
                # Corrige o valor de unit_hga
                df_edd_results.at[row_edd.name, 'unit_hga'] = row_merge['hga_unit']
                # Verificar se já existe um comentário e concatenar ao existente
                comentario_anterior = df_edd_results.at[row_edd.name, 'comentario_correcao']
                comentario = f"Correção: unit_hga ajustado de '{valor_anterior}' para '{row_merge['hga_unit']}' conforme De-Para."
                # Adicionar "/" apenas se houver um comentário anterior
                if pd.notna(comentario_anterior) and comentario_anterior != "":
                    comentario = comentario_anterior + " / " + comentario
                return 'corrigido', comentario
            else:
                return None, None

        # Verificar unidades no df_edd_results
        if 'comentario_correcao' not in df_edd_results.columns:
            df_edd_results['comentario_correcao'] = ""
        if 'acao' not in df_edd_results.columns:
            df_edd_results['acao'] = ""

        # Modificação no código para verificar se é string antes de aplicar lower()
        df_edd_results['unit_org'] = df_edd_results['unit_org'].apply(lambda x: str(x).strip().lower() if isinstance(x, str) else x)
        df_depara_units['original_unit'] = df_depara_units['original_unit'].apply(lambda x: str(x).strip().lower() if isinstance(x, str) else x)

        # Realizar o merge para verificar correspondência das unidades
        merged_units_df = pd.merge(
            df_edd_results[['unit_org', 'unit_hga', 'result_value']],
            df_depara_units[['original_unit', 'hga_unit']],
            left_on='unit_org',
            right_on='original_unit',
            how='left',
            indicator=True
        )

        # Aplicar a verificação e correção de unidades em df_edd_results
        for index, row_edd in df_edd_results.iterrows():
            row_merge = merged_units_df.iloc[index]
            acao, comentario = verificar_e_corrigir_units(row_edd, row_merge, index)
            if acao:
                df_edd_results.at[index, 'acao'] = acao
                df_edd_results.at[index, 'comentario_correcao'] = comentario

        return df_edd_results

    except Exception as e:
        print(f"Error verifying units: {e}")
        raise