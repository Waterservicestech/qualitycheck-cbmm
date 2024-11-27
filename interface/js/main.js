$(document).ready(function () {
  
  // Função para abrir o diálogo de seleção de pasta
  $("#upload-btn-input").click(async function () {
    try {
      let title = "Selecione a pasta";
      let file = await eel.getFolder(title)();
      $("#output_path").val(file);
    } catch (err) {
      alert(`Erro ao selecionar pasta: ${err}`);
    }
  });

  // Função para abrir o diálogo de seleção de arquivo EDD
  $("#edd-file-upload-btn").click(async function () {
    try {
      let title = "Selecione o arquivo EDD";
      let file = await eel.getFileEDD(title)();
      $("#edd-file-input").val(file);  // Exibe o caminho do arquivo no campo de input
      window.selectedEDDFile = file;  // Armazena o caminho completo do arquivo EDD
    } catch (err) {
      alert(`Erro ao selecionar o arquivo EDD: ${err}`);
    }
  });

  // Função para abrir o diálogo de seleção de arquivo De-Para
  $("#depara-file-upload-btn").click(async function () {
    try {
      let title = "Selecione o arquivo De-Para";
      let file = await eel.getFileEDD(title)();
      $("#depara-file-input").val(file);  // Exibe o caminho do arquivo no campo de input
      window.selectedDeParaFile = file;  // Armazena o caminho completo do arquivo De-Para
    } catch (err) {
      alert(`Erro ao selecionar o arquivo De-Para: ${err}`);
    }
  });

  // Função que será chamada pelo backend via Eel para mostrar mensagem de sucesso
  eel.expose(show_export_success);
  function show_export_success(message) {
    alert(message);
  }

  // Test-Connection Button Click
  $("#test-connection").click(async function () {
    try {
      let server_name = $("#server_name").val();
      let database_name = $("#database_name").val();
      let response = await eel.valid_connection(server_name, database_name)();

      if (response.success) {
        $("#info-conexao")[0].innerText = response.message;
        $("#info-conexao").addClass("text-success");
        
        // Exibir a seção EDD e De-Para
        $("#edd-section").removeClass("hidden");
        $("#tipo-dado-radio").removeClass("hidden");
      } else {
        $("#info-conexao")[0].innerText = response.message;
        $("#info-conexao").addClass("text-danger");

        // Ocultar a parte da seleção do Arquivo EDD se o teste de conexão falhar
        $("#edd-section").addClass("hidden");
      }
    } catch (err) {
      alert(`Erro ao testar conexão: ${err}`);
    }
  });

  // Verificar o EDD e De-Para ao clicar no botão "Validar Arquivos"
  $("#verificar-edd-btn").click(async function () {
    if (window.selectedEDDFile && window.selectedDeParaFile) { 
      try {
        const serverName = $("#server_name").val();
        const databaseName = $("#database_name").val();
        
        // Obter o tipo de dado selecionado
        const tipoDado = $("input[name='tipo_dado']:checked").val();

        // Mostrar o GIF de carregamento
        $("#loading-gif").show();

        // Chama a função Python e passa ambos os arquivos e o tipo de dado como argumentos
        let result = await eel.verificar_pontos_edd(window.selectedEDDFile, window.selectedDeParaFile, serverName, databaseName, tipoDado)();
        
        // Quando o processamento terminar, ocultar o GIF de carregamento
        $("#loading-gif").hide();

        if (result.success) {
          $("#status-edd").text(result.message);
        } else {
          $("#status-edd").text(`Erro: ${result.message}`);
        }
      } catch (err) {
        $("#loading-gif").hide();
        alert(`Erro ao validar arquivos: ${err}`);
      }
    } else {
      alert('Por favor, selecione ambos os arquivos EDD e De-Para.');
    }
  });
});
