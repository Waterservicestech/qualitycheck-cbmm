import pandas as pd

def process_edd(edd_file_path, depara_file_path, tipoDado):
    """
    Processes and validates EDD files based on mappings and database rules.

    Args:
        edd_file_path (str): Path to the EDD Excel file.
        depara_file_path (str): Path to the De-Para mapping Excel file.
        tipoDado (str): Type of data being validated ('agua' or 'solo').

    Returns:
        tuple: DataFrames with renamed columns (`df_edd_station`, `df_edd_results`) and the mapping DataFrames.
    """
    try:
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
        return df_edd_station, df_edd_results, df_depara_parameters, df_depara_sample, df_depara_units

    except Exception as e:
        print(f"Error processing EDD: {e}")
        raise
