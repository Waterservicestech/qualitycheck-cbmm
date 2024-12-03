import pandas as pd

def depara_results(df_edd_results, df_depara_parameters):
    """
    Validates and corrects parameters (parameter_hga and parameter_group) in EDD results based on De-Para mappings.

    Args:
        df_edd_results (DataFrame): EDD results data.
        df_depara_parameters (DataFrame): Mapping of parameters.

    Returns:
        None: Updates the `df_edd_results` in place.
    """
    try:
        ####### DE-PARA RESULTS #######
        def verificar_e_corrigir_results(row_edd, row_merge):
            if row_merge['_merge'] == 'left_only':
                return 'erro', 'Não encontrado correspondência no de-para para parameter_org'

            # Verificar e corrigir parameter_hga e parameter_group
            correcoes = []
            if row_edd['parameter_hga'] != row_merge['parameter_hga_y']:
                valor_anterior_parametro = row_edd['parameter_hga']
                df_edd_results.at[row_edd.name, 'parameter_hga'] = row_merge['parameter_hga_y']
                correcoes.append(f"parameter_hga ajustado de '{valor_anterior_parametro}' para '{row_merge['parameter_hga_y']}'")

            if row_edd['parameter_group'] != row_merge['parameter_group_y']:
                valor_anterior_grupo = row_edd['parameter_group']
                df_edd_results.at[row_edd.name, 'parameter_group'] = row_merge['parameter_group_y']
                correcoes.append(f"parameter_group ajustado de '{valor_anterior_grupo}' para '{row_merge['parameter_group_y']}'")

            if correcoes:
                comentario_anterior = df_edd_results.at[row_edd.name, 'comentario_correcao']
                comentario = "Correção: " + ", ".join(correcoes)
                if pd.notna(comentario_anterior) and comentario_anterior != "":
                    comentario = comentario_anterior + " / " + comentario
                return 'corrigido', comentario
            else:
                return None, None

        # Realizar o merge para verificar correspondência dos parâmetros
        merged_results_df = pd.merge(
            df_edd_results[['parameter_org', 'parameter_hga', 'parameter_group']],
            df_depara_parameters[['parameter_org', 'parameter_hga', 'parameter_group']],
            on='parameter_org',
            how='left',
            indicator=True
        )

        # Aplicar a verificação e correção em df_edd_results
        for index, row_edd in df_edd_results.iterrows():
            row_merge = merged_results_df.iloc[index]
            acao, comentario = verificar_e_corrigir_results(row_edd, row_merge)
            if acao:
                df_edd_results.at[index, 'acao'] = acao
                df_edd_results.at[index, 'comentario_correcao'] = comentario

        return df_edd_results

    except Exception as e:
        print(f"Error verifying results: {e}")
        raise
