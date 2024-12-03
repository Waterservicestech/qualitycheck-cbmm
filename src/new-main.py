import eel
from init.init_conn import connect_to_database
from quality.transform.process_edd import process_edd
from quality.transform.depara_results import depara_results
from quality.transform.depara_sample import depara_sample
from quality.transform.depara_units import depara_units
from quality.validate.check_list_round import check_list_round
from quality.validate.check_station import check_station
from quality.validate.check_duplicate import check_duplicate
from quality.validate.check_date import check_date
from quality.validate.check_sp_charac import check_sp_charac
from quality.export.export import export, get_resource_path

@eel.expose
def quality_validation(edd_file_path, depara_file_path, server_name, database_name, tipo_dado):
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
        df_edd_station, df_edd_results, df_depara_parameters, df_depara_sample, df_depara_units = process_edd(edd_file_path, depara_file_path, tipo_dado)
        # De Para
        df_edd_results = depara_units(df_edd_results, df_depara_units)
        df_edd_results = depara_results(df_edd_results, df_depara_parameters)
        df_edd_station = depara_sample(df_edd_station, df_depara_sample)
        # Validações
        df_edd_station = check_list_round(depara_file_path, df_edd_station, tipo_dado)
        df_edd_station = check_station(conn, df_edd_station)
        df_edd_station = check_duplicate(conn, df_edd_station, tipo_dado)
        df_edd_station, df_edd_results = check_date(df_edd_station, df_edd_results)
        df_edd_station, df_edd_results = check_sp_charac(df_edd_station, df_edd_results)
        # Exportar resultados
        export(df_edd_station, df_edd_results, edd_file_path, tipo_dado)

    except Exception as e:
        print(f"Erro geral no processamento do EDD: {e}")
        eel.show_export_success(f"Erro geral: {str(e)}")


eel.init(get_resource_path('interface'), allowed_extensions=['.js', '.html'])
eel.start('index.html', mode='default', cmdline_args=['--disable-translate'])
