import eel

def process_edd_files(edd_file_path, depara_file_path, server_name, database_name, tipo_dado):
    """
    Orquestra todas as etapas de validação e correção nos arquivos EDD e De-Para.

    Args:
        edd_file_path (str): Caminho do arquivo EDD.
        depara_file_path (str): Caminho do arquivo De-Para.
        server_name (str): Nome do servidor.
        database_name (str): Nome do banco de dados.
        tipo_dado (str): Tipo de dado ('agua' ou 'solo').

    Returns:
        tuple: DataFrames atualizados (`df_edd_station`, `df_edd_results`) e caminho do arquivo exportado.
    """
    try:
        # Conectar ao banco de dados
        conn = connect_to_database(server_name, database_name)

        # Ler os DataFrames do EDD e De-Para
        df_edd_station, df_edd_results, df_depara_parameters, df_depara_sample, df_depara_units = process_edd(edd_file_path, depara_file_path, tipoDado)

        df_edd_results = verificar_e_corrigir_units(df_edd_results, df_depara_units)
        df_edd_results = verificar_e_corrigir_results(df_edd_results, df_depara_parameters)
        df_edd_station = verificar_e_corrigir_sample(df_edd_station, df_depara_sample)
        # Verificar listas obrigatórias
        df_edd_station, df_edd_results = verificar_lists(
            df_edd_station, df_edd_results, depara_file_path, tipo_dado
        )

        # Verificar nomes de estações no banco de dados
        df_edd_station = station_name_verify(conn, df_edd_station, df_edd_results, tipo_dado)

        # Verificar duplicatas de amostras no banco de dados
        df_edd_station, df_edd_results = verificar_duplicates(df_edd_station, df_edd_results, conn, tipo_dado)

        # Verificar datas e formatos
        df_edd_station, df_edd_results = verificar_dates(df_edd_station, df_edd_results)

        # Verificar caracteres especiais
        df_edd_station, df_edd_results = verificar_special_characters(df_edd_station, df_edd_results)

        # Exportar resultados
        export(df_edd_station, df_edd_results, edd_file_path, tipo_dado)

    except Exception as e:
        print(f"Erro geral no processamento do EDD: {e}")
        eel.show_export_success(f"Erro geral: {str(e)}")


if __name__ == "__main__":
    eel.init('interface', allowed_extensions=['.js', '.html'])
    eel.start('index.html', mode='default', cmdline_args=['--disable-translate'])
