from tkinter import Tk, filedialog
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
import pandas as pd
import eel
import re
import os
import sys
import pyodbc

@eel.expose
def getFileEDD(path=''):
    """Esta função obtém o caminho do arquivo EDD selecionado pelo usuário.

    Args:
        path (str, optional): caminho inicial ou título da janela de diálogo. Padrão é ''.

    Returns:
        str: caminho completo do arquivo na máquina do usuário
    """
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    file = filedialog.askopenfilename(
        title=path,
        filetypes=[("Arquivos Excel", "*.xlsx *.xls")]
    )
    file = file if type(file) != tuple else ''
    file = str(Path(file))
    file = file if file != '.' else ''
    return file

@eel.expose
def getFolder(path=''):
    """Esta função obtém o caminho da pasta para salvar os
    arquivos PDF ou qualquer outro arquivo.

    Args:
        path (str, optional): caminho para a pasta. Padrão é ''.

    Returns:
        str: caminho completo da pasta na máquina do usuário
    """
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    folder = filedialog.askdirectory(title=path)
    folder = folder if type(folder) != tuple else ''
    folder = str(Path(folder))
    folder = folder if folder != '.' else ''
    return folder

def check_any_odbc_driver():
    drivers = [driver for driver in pyodbc.drivers()]
    for driver in drivers:
        if "ODBC Driver" in driver and "SQL Server" in driver:
            return driver  # Retorna o nome do driver encontrado
    return None

# Função para conectar ao banco de dados com base no driver encontrado
def connect_to_database(server_name, database_name):
    """Função que estabelece conexão com o banco de dados usando um driver ODBC encontrado."""
    odbc_driver = check_any_odbc_driver()

    # Caso não encontre nenhum driver ODBC
    if not odbc_driver:
        eel.show_export_success("Erro: Nenhum driver ODBC adequado encontrado")
        return None

    connection_str = f'Driver={{{odbc_driver}}};Server={server_name};Database={database_name};Trusted_Connection=yes;Encrypt=no;'
    connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_str})
    conn = create_engine(connection_url)
    return conn

@eel.expose
def valid_connection(server_name, database_name):
    """Função para testar a conexão com um servidor.
    Retorna True se a consulta de teste puder ser feita.

    Args:
        server_name (str): Nome do servidor para conectar
        database_name (str): Nome do banco de dados

    Returns:
        boolean: True/False se amostra de dados foi recuperada
    """
    try:
        conn = connect_to_database(server_name, database_name)
        test_query = 'SELECT TOP 10 [ID] FROM station'
        df_test = pd.read_sql(test_query, conn)
        return True
    except Exception as e:
        print(e)
        return False

@eel.expose
def verificar_pontos_edd(edd_file_path, depara_file_path, server_name, database_name, tipoDado):
    """Função para renomear colunas do arquivo EDD com base nas instruções da aba 'columns' do arquivo De-Para e corrigir unidades."""
    try:
        # Conectar ao banco de dados
        conn = connect_to_database(server_name, database_name)

        # Definir o nome das abas dinamicamente com base no tipo de dado
        if tipoDado.lower() == 'agua':
            sample_sheet_name = 'Monitoring_Sample'
            result_sheet_name = 'Monitoring_Sample_Result'
        elif tipoDado.lower() == 'solo':
            sample_sheet_name = 'Soil_Sample'
            result_sheet_name = 'Soil_Result'
        else:
            raise ValueError(f"TipoDado inválido: {tipoDado}")

        # Ler os arquivos Excel EDD e De-Para
        df_edd_station = pd.read_excel(edd_file_path, sheet_name=sample_sheet_name)
        df_edd_results = pd.read_excel(edd_file_path, sheet_name=result_sheet_name)
        df_depara_columns = pd.read_excel(depara_file_path, sheet_name='Columns')
        df_depara_parameters = pd.read_excel(depara_file_path, sheet_name='Parameters')
        df_depara_sample = pd.read_excel(depara_file_path, sheet_name='Station')
        df_depara_units = pd.read_excel(depara_file_path, sheet_name='Units')

        # Renomear as colunas dos parâmetros
        df_depara_parameters.rename(columns={
            'Parametro Padrao': 'parameter_hga', 
            'Unidade Padrao': 'unit_hga', 
            'Parametro Original': 'parameter_org', 
            'Unidade Original': 'unit_org', 
            'Grupo Padrao': 'parameter_group'
        }, inplace=True)

        # Renomear as colunas do de-para da estação
        df_depara_sample.rename(columns={
            'Sample Name (Nome no Laboratorio)': 'sample_name', 
            'Codigo HGA': 'name'
        }, inplace=True)


        # Renomear colunas de acordo com de-para columns
        rename_station = df_depara_columns[df_depara_columns['Tabela'] == 'Sample'].set_index('Nome da Coluna no EDD')['Padrao Formatador'].to_dict()
        rename_results = df_depara_columns[df_depara_columns['Tabela'] == 'Result'].set_index('Nome da Coluna no EDD')['Padrao Formatador'].to_dict()

        df_edd_station.rename(columns=rename_station, inplace=True)
        df_edd_results.rename(columns=rename_results, inplace=True)
        
        df_edd_station['sample_name_original'] = df_edd_station['sample_name']

        ####### DE-PARA UNITS #######
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

        ####### DE-PARA SAMPLE #######
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

        # Chamando outras verificações (se necessário)
        lists_verify(depara_file_path, conn, df_edd_station, df_edd_results, tipoDado, edd_file_path)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

@eel.expose
def lists_verify(depara_file_path, conn, df_edd_station, df_edd_results, tipoDado, edd_file_path):
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

        # Chamar a verificação de station_name no banco de dados
        station_name_verify(conn, df_edd_station, df_edd_results, tipoDado, edd_file_path)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

def station_name_verify(conn, df_edd_station, df_edd_results, tipoDado, edd_file_path):
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

                # Usar a função UPPER e TRIM para garantir que os espaços em branco sejam removidos
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

        # Continuar para verificar duplicatas (se necessário)
        sample_duplicate_verify(conn, df_edd_station, df_edd_results, tipoDado, edd_file_path)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")


@eel.expose
def sample_duplicate_verify(conn, df_edd_station, df_edd_results, tipoDado, edd_file_path):
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

        result_value_verify(conn, df_edd_station, df_edd_results, tipoDado, edd_file_path)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

@eel.expose
def result_value_verify(conn, df_edd_station, df_edd_results, tipoDado, edd_file_path):
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

        date_verify(df_edd_results, df_edd_station, edd_file_path, tipoDado)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

from datetime import datetime

@eel.expose
def date_verify(df_edd_results, df_edd_station, edd_file_path, tipoDado):
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

        special_characters(df_edd_results, df_edd_station, edd_file_path, tipoDado)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

@eel.expose
def special_characters(df_edd_results, df_edd_station, edd_file_path, tipoDado):
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

        export(df_edd_station, df_edd_results, edd_file_path, tipoDado)

    except Exception as e:
        print(f"Erro geral: {e}")
        eel.show_export_success(f"Erro: {str(e)}")

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

eel.init(get_resource_path('interface'), allowed_extensions=['.js', '.html'])
eel.start('index.html', mode='default', cmdline_args=['--disable-translate'])