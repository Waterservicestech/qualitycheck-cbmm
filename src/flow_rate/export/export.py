import os
from datetime import datetime
import pandas as pd
import eel
import sys

def export(df_edd_station, df_edd_results, edd_file_path, tipoDado):
    """
    Exporta as linhas dos DataFrames df_edd_station e df_edd_results que possuem 'erro' na coluna 'acao'
    para novas abas em um arquivo Excel, e cria uma cópia do arquivo original com o sufixo '_validated_' + timestamp.
    Também exclui as linhas com 'erro' das abas 'Sample' e 'Results'.

    Args:
        df_edd_station (DataFrame): DataFrame que contém as amostras da aba 'Sample'.
        df_edd_results (DataFrame): DataFrame que contém os resultados da aba 'Results'.
        edd_file_path (str): Caminho do arquivo EDD original.
        tipoDado (str): Tipo de dado (Água ou Solo) para definir a tabela a ser usada.

    Returns:
        None: Cria uma cópia do arquivo Excel com as validações.
    """
    try:
        # Gerar timestamp para adicionar ao nome do arquivo
        timestamp = datetime.now().strftime('%d%m%Y%H%M')
        base_name, ext = os.path.splitext(edd_file_path)
        new_file_path = f"{base_name}_validated_{timestamp}{ext}"

        # Filtrar as linhas que CONTÊM 'erro' na coluna 'acao' (mesmo que tenha outras palavras)
        df_sample_error = df_edd_station[df_edd_station['acao'].str.contains('erro', na=False, case=False)]
        df_results_error = df_edd_results[df_edd_results['acao'].str.contains('erro', na=False, case=False)]

        # Filtrar as linhas que NÃO CONTÊM 'erro' na coluna 'acao'
        df_sample_valid = df_edd_station[~df_edd_station['acao'].str.contains('erro', na=False, case=False)]
        df_results_valid = df_edd_results[~df_edd_results['acao'].str.contains('erro', na=False, case=False)]

        # Definir o nome das abas com base no tipo de dado
        if tipoDado.lower() == 'agua':
            sample_sheet_name = 'Monitoring_Sample'
            result_sheet_name = 'Monitoring_Sample_Result'
        elif tipoDado.lower() == 'solo':
            sample_sheet_name = 'Soil_Sample'
            result_sheet_name = 'Soil_Result'
        else:
            raise ValueError(f"TipoDado inválido: {tipoDado}")

        # Criar uma cópia do arquivo original e adicionar as novas abas
        with pd.ExcelWriter(new_file_path, engine='openpyxl', mode='w') as writer:
            # Exportar as linhas válidas (sem erro) para as abas 'Sample' e 'Results'
            df_sample_valid.to_excel(writer, sheet_name=sample_sheet_name, index=False)
            df_results_valid.to_excel(writer, sheet_name=result_sheet_name, index=False)

            # Exportar as linhas de erro para novas abas
            df_sample_error.to_excel(writer, sheet_name='Sample_Error', index=False)
            df_results_error.to_excel(writer, sheet_name='Results_Error', index=False)

        # Enviar uma mensagem de sucesso de volta à interface
        eel.show_export_success(f"Validação concluída e salva em: '{new_file_path}'")

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

def get_resource_path(relative_path):
    """Obter o caminho correto para os recursos (HTML, CSS, etc.) ao usar PyInstaller"""
    try:
        # Quando executado como um executável criado pelo PyInstaller
        base_path = sys._MEIPASS
    except Exception:
        # Quando executado normalmente pelo Python
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)