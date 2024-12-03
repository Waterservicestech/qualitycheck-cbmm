$(document).ready(function () {
  // Test-Connection Button Click
  $("#test-connection").click(async function () {
    try {
      let server_name = $("#server_name").val();
      let database_name = $("#database_name").val();
      let response = await eel.valid_connection(server_name, database_name)();

      // Remove classes existentes antes de adicionar a nova
      $("#info-conexao").removeClass("text-success text-danger");

      if (response.success) {
        $("#info-conexao")[0].innerText = response.message;
        $("#info-conexao").addClass("text-success");

        // Exibir a seleção de tipo de dado
        $("#tipo-dado-principal").removeClass("hidden");
      } else {
        $("#info-conexao")[0].innerText = response.message;
        $("#info-conexao").addClass("text-danger");

        // Ocultar a seleção de tipo de dado se o teste de conexão falhar
        $("#tipo-dado-principal").addClass("hidden");
        $("#edd-section").addClass("hidden");
        $("#tipo-dado-radio").addClass("hidden"); // Certifique-se de esconder o tipo-dado-radio também
      }
    } catch (err) {
      alert(`Erro ao testar conexão: ${err}`);
    }
  });

  // Quando o botão "Selecionar" for clicado
  $("#selecionar-tipo-dado").click(function () {
    const tipoDadoSelecionado = $("input[name='tipo_dado_principal']:checked").val();

    // Exibir ou ocultar tipo-dado-radio baseado na seleção
    if (tipoDadoSelecionado === "qualidade") {
      $("#tipo-dado-radio").removeClass("hidden"); // Mostrar opções de Águas/Solo
      $("#edd-section").removeClass("hidden");
      $("#depara-section").removeClass("hidden");
      $("#verificar-edd-btn").removeClass("hidden");
    } else if (tipoDadoSelecionado === "vazao") {
      $("#tipo-dado-radio").addClass("hidden"); // Ocultar opções de Águas/Solo para dados de vazão
      $("#edd-section").removeClass("hidden");
      $("#depara-section").addClass("hidden"); // Esconder campo De-Para
      $("#verificar-edd-btn").removeClass("hidden");
    }
  });

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
      $("#edd-file-input").val(file);
      window.selectedEDDFile = file;
    } catch (err) {
      alert(`Erro ao selecionar o arquivo EDD: ${err}`);
    }
  });

  // Função para abrir o diálogo de seleção de arquivo De-Para
  $("#depara-file-upload-btn").click(async function () {
    try {
      let title = "Selecione o arquivo De-Para";
      let file = await eel.getFileEDD(title)();
      $("#depara-file-input").val(file);
      window.selectedDeParaFile = file;
    } catch (err) {
      alert(`Erro ao selecionar o arquivo De-Para: ${err}`);
    }
  });

  // Verificar o EDD e De-Para ao clicar no botão "Validar Arquivos"
  $("#verificar-edd-btn").click(async function () {
    if (window.selectedEDDFile && (window.selectedDeParaFile || $("#tipo-dado-radio").hasClass("hidden"))) {
      try {
        const serverName = $("#server_name").val();
        const databaseName = $("#database_name").val();

        // Obter o tipo de dado selecionado
        const tipoDado = $("input[name='tipo_dado_principal']:checked").val();
        const tipoQualidade = $("input[name='tipo_dado']:checked").val(); // Para águas e solo, se visível

        // Mostrar o GIF de carregamento
        $("#loading-gif").show();

        // Chama a função Python
        let result = await eel.quality_validation(
          window.selectedEDDFile,
          window.selectedDeParaFile || null,
          serverName,
          databaseName,
          tipoDado === "qualidade" ? tipoQualidade : tipoDado
        )();

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
      alert("Por favor, selecione os arquivos necessários.");
    }
  });
});
