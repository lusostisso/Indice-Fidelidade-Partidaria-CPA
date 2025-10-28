import requests
import time
import json
import os
import calendar
from datetime import datetime

# --- Configurações ---
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2/votacoes"
ANO_INICIO = 2018
ANO_FIM = 2022
ITENS_POR_PAGINA = 100
PASTA_OUTPUT = "dados_votacoes"
TEMPO_ESPERA_SEC = 0.5        # Espera normal entre requisições BEM-SUCEDIDAS
TEMPO_ESPERA_RETRY_SEC = 10   # Espera mais longa após um ERRO (5xx)
MAX_TENTATIVAS = 5            # Máximo de tentativas por página antes de desistir

# Cria a pasta de saída se ela não existir
os.makedirs(PASTA_OUTPUT, exist_ok=True)

def get_last_day_of_month(ano, mes):
    """
    Retorna o último dia do mês, lidando corretamente com anos bissextos.
    """
    # calendar.monthrange(ano, mes) retorna (dia_da_semana_do_dia_1, num_dias_no_mes)
    _, num_dias = calendar.monthrange(ano, mes)
    return num_dias

def coletar_dados_ano(ano):
    """
    Coleta todas as votações para um ano, quebrando a busca 
    em intervalos de 1 MÊS para evitar erros na API.
    """
    print(f"--- Iniciando coleta para o ano: {ano} ---")
    
    # Lista para armazenar todos os dados do ano
    votacoes_do_ano = []
    
    # Loop INTERMEDIÁRIO: Itera por MÊS (1 a 12)
    for mes in range(1, 13):
        
        # Define as datas de início e fim do mês
        data_inicio = f"{ano}-{mes:02d}-01"
        ultimo_dia = get_last_day_of_month(ano, mes)
        data_fim = f"{ano}-{mes:02d}-{ultimo_dia:02d}"
        
        print(f"\nBuscando intervalo: {data_inicio} até {data_fim}")
        
        # Loop INTERNO: Paginação
        pagina_atual = 1
        while True:
            
            # Loop de TENTATIVAS: Tenta até MAX_TENTATIVAS
            tentativa_atual = 1
            while tentativa_atual <= MAX_TENTATIVAS:
                
                # Monta os parâmetros da requisição
                params = {
                    'dataInicio': data_inicio,
                    'dataFim': data_fim,
                    'itens': ITENS_POR_PAGINA,
                    'pagina': pagina_atual
                }
                
                print(f"   Buscando Ano {ano}, Mês {mes:02d}, Página {pagina_atual} (Tentativa {tentativa_atual}/{MAX_TENTATIVAS})...")
                
                try:
                    # Faz a requisição GET
                    response = requests.get(BASE_URL, params=params, headers={'Accept': 'application/json'})
                    
                    # --- Caso 1: Sucesso (Status 200) ---
                    if response.status_code == 200:
                        data = response.json()
                        votacoes_da_pagina = data.get('dados', [])
                        
                        # Verifica se a página está vazia (fim dos resultados)
                        if not votacoes_da_pagina:
                            print(f"      Fim dos resultados para este mês.")
                            break # Sai do loop de paginação (while True)
                        
                        # Adiciona os dados e vai para a próxima página
                        votacoes_do_ano.extend(votacoes_da_pagina)
                        print(f"      OK! {len(votacoes_da_pagina)} itens recebidos.")
                        
                        pagina_atual += 1          # Prepara para a próxima página
                        time.sleep(TEMPO_ESPERA_SEC) # Espera normal
                        break # Sai do loop de TENTATIVAS e continua o loop de paginação
                    
                    # --- Caso 2: Erro de Servidor (500, 502, 503, 504) ---
                    elif response.status_code in [500, 502, 503, 504]:
                        print(f"      Erro de Servidor! Status {response.status_code} ({response.reason}).")
                        print(f"      Resposta: {response.text}")
                        tentativa_atual += 1
                        print(f"      Aguardando {TEMPO_ESPERA_RETRY_SEC}s para tentar novamente...")
                        time.sleep(TEMPO_ESPERA_RETRY_SEC)
                        # O loop de TENTATIVAS continuará
                    
                    # --- Caso 3: Outro Erro (404, 400, etc.) ---
                    else:
                        print(f"      Erro inesperado! Status {response.status_code}. Abortando este mês.")
                        print(f"      Resposta: {response.text}")
                        break # Sai do loop de TENTATIVAS e (abaixo) do loop de paginação

                except requests.exceptions.RequestException as e:
                    # --- Caso 4: Erro de Conexão (Internet caiu, DNS, etc.) ---
                    print(f"      Erro de Conexão: {e}")
                    tentativa_atual += 1
                    print(f"      Aguardando {TEMPO_ESPERA_RETRY_SEC}s para tentar novamente...")
                    time.sleep(TEMPO_ESPERA_RETRY_SEC)
                    # O loop de TENTATIVAS continuará

            # --- Fim do loop de TENTATIVAS ---
            
            # Se saiu do loop de tentativas por falha, aborta o mês
            if tentativa_atual > MAX_TENTATIVAS:
                print(f"ERRO CRÍTICO: Falha ao buscar página {pagina_atual} do Mês {mes}/{ano} após {MAX_TENTATIVAS} tentativas.")
                print("ABORTANDO este mês e pulando para o próximo.")
                break # Sai do loop de paginação (while True)

            # Se saiu por um erro inesperado (ex: 404)
            if response.status_code != 200:
                break # Sai do loop de paginação (while True)
                
            # Se saiu por 'not votacoes_da_pagina'
            if not votacoes_da_pagina:
                break # Sai do loop de paginação (while True)
                
    # --- Fim do loop de MÊS ---
    
    # Salva os dados consolidados do ano em um arquivo JSON
    if votacoes_do_ano:
        file_path = os.path.join(PASTA_OUTPUT, f"votacoes_{ano}.json")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(votacoes_do_ano, f, ensure_ascii=False, indent=4)
            print(f"\n--- DADOS SALVOS: {file_path} (Total de {len(votacoes_do_ano)} itens para o ano {ano}) ---")
            return len(votacoes_do_ano)
        except IOError as e:
            print(f"ERRO AO SALVAR ARQUIVO {file_path}: {e}")
            return 0
    else:
        print(f"\n--- Nenhum dado encontrado para o ano {ano} ---")
        return 0

# --- Execução Principal ---
if __name__ == "__main__":
    total_geral_itens = 0
    
    # Loop EXTERNO: Itera por cada ano
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        itens_coletados_no_ano = coletar_dados_ano(ano)
        total_geral_itens += itens_coletados_no_ano

    print("\n===================================")
    print("      COLETA CONCLUÍDA")
    print("===================================")
    print(f"Anos processados: {ANO_INICIO} a {ANO_FIM}")
    print("Um ou mais identificador(es) numéricos de órgãos da Câmara, separados por vírgulas. Se presente, serão retornadas somente votações dos órgãos enumerados. Os identificadores existentes podem ser obtidos por meio do recurso /orgaos.")
    print(f"Os dados estão salvos na pasta: {PASTA_OUTPUT}")