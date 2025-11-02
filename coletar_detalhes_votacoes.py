import requests
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from urllib.parse import urlparse, parse_qs
import re

# --- Configurações ---
BASE_URL_API = "https://dadosabertos.camara.leg.br/api/v2"
ANO_INICIO = 2018
ANO_FIM = 2022
PASTA_INPUT = "dados_votacoes"
PASTA_OUTPUT = "dados_votacoes"
TEMPO_ESPERA_SEC = 0.5        # Espera normal entre requisições (aumentado)
TEMPO_ESPERA_RETRY_SEC = 10   # Espera após erro (aumentado)
MAX_TENTATIVAS = 5            # Máximo de tentativas por requisição (aumentado)
MAX_WORKERS = 4               # Número de threads para paralelização (reduzido)

# Lock para thread-safe writing
write_lock = threading.Lock()
progress_lock = threading.Lock()

# Contadores globais
total_processados = 0
total_erros = 0

def extrair_id_da_uri(uri):
    """
    Extrai o ID numérico de uma URI da API da Câmara.
    Exemplo: 'https://dadosabertos.camara.leg.br/api/v2/proposicoes/2270857' -> '2270857'
    """
    if not uri:
        return None
    
    # Tenta extrair o último número da URI
    match = re.search(r'/(\d+)/?$', uri)
    if match:
        return match.group(1)
    return None

def fazer_requisicao_com_retry(url, descricao=""):
    """
    Faz uma requisição HTTP com sistema de retry.
    """
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            response = requests.get(url, headers={'Accept': 'application/json'}, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # 404 é esperado para algumas proposições/votações que não existem mais
                return None
            elif response.status_code in [429, 500, 502, 503, 504]:
                # Rate limit ou erro de servidor - retry
                if tentativa < MAX_TENTATIVAS:
                    time.sleep(TEMPO_ESPERA_RETRY_SEC * tentativa)
                    continue
                else:
                    print(f"      ERRO {response.status_code} após {MAX_TENTATIVAS} tentativas: {descricao}")
                    return None
            else:
                print(f"      Erro HTTP {response.status_code}: {descricao}")
                return None
                
        except requests.exceptions.RequestException as e:
            if tentativa < MAX_TENTATIVAS:
                time.sleep(TEMPO_ESPERA_RETRY_SEC)
                continue
            else:
                print(f"      Erro de conexão após {MAX_TENTATIVAS} tentativas: {descricao} - {e}")
                return None
    
    return None

def processar_votacao_id(votacao_id, ano):
    """
    Processa uma votação específica, coletando seus detalhes e temas das proposições relacionadas.
    """
    global total_processados, total_erros
    
    try:
        # 1. Buscar detalhes da votação
        url_votacao = f"{BASE_URL_API}/votacoes/{votacao_id}"
        dados_votacao = fazer_requisicao_com_retry(url_votacao, f"Votação {votacao_id}")
        
        if not dados_votacao:
            with progress_lock:
                total_erros += 1
            return None, None
        
        # Extrair dados da votação
        votacao_detalhes = dados_votacao.get('dados', {})
        
        # 2. Coletar todas as proposições relacionadas de diferentes fontes
        proposicoes_relacionadas = []
        
        # A) uriProposicaoObjeto (se existir)
        uri_proposicao_objeto = votacao_detalhes.get('uriProposicaoObjeto')
        if uri_proposicao_objeto:
            prop_id = extrair_id_da_uri(uri_proposicao_objeto)
            if prop_id and prop_id not in proposicoes_relacionadas:
                proposicoes_relacionadas.append(prop_id)
        
        # B) ultimaApresentacaoProposicao (muito comum)
        ultima_apresentacao = votacao_detalhes.get('ultimaApresentacaoProposicao', {})
        if ultima_apresentacao:
            uri_proposta = ultima_apresentacao.get('uriProposicaoCitada')
            if uri_proposta:
                prop_id = extrair_id_da_uri(uri_proposta)
                if prop_id and prop_id not in proposicoes_relacionadas:
                    proposicoes_relacionadas.append(prop_id)
        
        # C) objetosPossiveis (array de proposições)
        objetos_possiveis = votacao_detalhes.get('objetosPossiveis', [])
        for objeto in objetos_possiveis:
            if isinstance(objeto, dict):
                uri_obj = objeto.get('uri')
                if uri_obj:
                    prop_id = extrair_id_da_uri(uri_obj)
                    if prop_id and prop_id not in proposicoes_relacionadas:
                        proposicoes_relacionadas.append(prop_id)
        
        # D) proposicoesAfetadas (array de proposições)
        proposicoes_afetadas = votacao_detalhes.get('proposicoesAfetadas', [])
        for proposicao in proposicoes_afetadas:
            if isinstance(proposicao, dict):
                uri_prop = proposicao.get('uri')
                if uri_prop:
                    prop_id = extrair_id_da_uri(uri_prop)
                    if prop_id and prop_id not in proposicoes_relacionadas:
                        proposicoes_relacionadas.append(prop_id)
        
        # 3. Buscar temas das proposições encontradas
        temas_proposicoes = {}
        for prop_id in proposicoes_relacionadas:
            # Buscar temas da proposição
            url_temas = f"{BASE_URL_API}/proposicoes/{prop_id}/temas"
            dados_temas = fazer_requisicao_com_retry(url_temas, f"Temas proposição {prop_id}")
            
            if dados_temas and 'dados' in dados_temas:
                temas = dados_temas.get('dados', [])
                if temas:  # só adiciona se tiver temas
                    temas_proposicoes[prop_id] = temas
            
            # Buscar detalhes básicos da proposição (título, ementa, etc.)
            url_proposicao = f"{BASE_URL_API}/proposicoes/{prop_id}"
            dados_proposicao = fazer_requisicao_com_retry(url_proposicao, f"Proposição {prop_id}")
            
            if dados_proposicao and 'dados' in dados_proposicao:
                prop_dados = dados_proposicao.get('dados', {})
                # Adiciona informações básicas da proposição aos temas
                if prop_id not in temas_proposicoes:
                    temas_proposicoes[prop_id] = []
                
                # Adiciona metadados da proposição
                if prop_id in temas_proposicoes or not temas_proposicoes[prop_id]:
                    temas_proposicoes[f"{prop_id}_info"] = {
                        'siglaTipo': prop_dados.get('siglaTipo'),
                        'numero': prop_dados.get('numero'),
                        'ano': prop_dados.get('ano'),
                        'ementa': prop_dados.get('ementa'),
                        'descricaoTipo': prop_dados.get('descricaoTipo')
                    }
            
            # Pequena pausa para evitar rate limiting
            time.sleep(TEMPO_ESPERA_SEC)
        
        # Adicionar os dados coletados ao objeto da votação
        votacao_detalhes['temasProposicoes'] = temas_proposicoes
        votacao_detalhes['proposicoesRelacionadas'] = proposicoes_relacionadas
        
        with progress_lock:
            total_processados += 1
            if total_processados % 50 == 0:  # Log mais frequente
                print(f"      Processados: {total_processados} | Erros: {total_erros} | Temas encontrados: {len([k for k in temas_proposicoes.keys() if not k.endswith('_info')])}")
        
        return votacao_id, votacao_detalhes
        
    except Exception as e:
        print(f"      Erro inesperado ao processar votação {votacao_id}: {e}")
        with progress_lock:
            total_erros += 1
        return None, None

def coletar_detalhes_ano(ano):
    """
    Coleta detalhes das votações e temas das proposições para um ano específico.
    """
    global total_processados, total_erros
    
    print(f"\n--- Iniciando coleta de detalhes para o ano: {ano} ---")
    
    # Carrega os dados básicos de votações do ano
    arquivo_votacoes = os.path.join(PASTA_INPUT, f"votacoes_{ano}.json")
    
    if not os.path.exists(arquivo_votacoes):
        print(f"Arquivo {arquivo_votacoes} não encontrado. Pulando ano {ano}.")
        return 0
    
    try:
        with open(arquivo_votacoes, 'r', encoding='utf-8') as f:
            votacoes_basicas = json.load(f)
    except Exception as e:
        print(f"Erro ao ler arquivo {arquivo_votacoes}: {e}")
        return 0
    
    # Extrai os IDs das votações
    votacao_ids = []
    for votacao in votacoes_basicas:
        votacao_id = votacao.get('id')
        if votacao_id:
            votacao_ids.append(votacao_id)
    
    if not votacao_ids:
        print(f"Nenhum ID de votação encontrado para o ano {ano}")
        return 0
    
    print(f"Encontradas {len(votacao_ids)} votações para processar")
    
    # Reset contadores para este ano
    total_processados = 0
    total_erros = 0
    
    # Processar votações em paralelo
    votacoes_detalhadas = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submete todas as tarefas
        future_to_votacao = {
            executor.submit(processar_votacao_id, votacao_id, ano): votacao_id 
            for votacao_id in votacao_ids
        }
        
        # Coleta os resultados conforme completam
        for future in as_completed(future_to_votacao):
            votacao_id, dados_detalhados = future.result()
            if dados_detalhados:
                votacoes_detalhadas[votacao_id] = dados_detalhados
    
    # Salva os dados coletados
    if votacoes_detalhadas:
        # Converte para lista para manter compatibilidade
        lista_votacoes = list(votacoes_detalhadas.values())
        
        arquivo_saida = os.path.join(PASTA_OUTPUT, f"votacoesID_{ano}.json")
        try:
            with open(arquivo_saida, 'w', encoding='utf-8') as f:
                json.dump(lista_votacoes, f, ensure_ascii=False, indent=2)
            
            print(f"\n--- DADOS SALVOS: {arquivo_saida} ---")
            print(f"    Total processado: {len(lista_votacoes)} votações")
            print(f"    Total com erro: {total_erros}")
            return len(lista_votacoes)
            
        except Exception as e:
            print(f"Erro ao salvar arquivo {arquivo_saida}: {e}")
            return 0
    else:
        print(f"Nenhum dado coletado para o ano {ano}")
        return 0

def main():
    """
    Função principal que coordena a coleta para todos os anos.
    """
    print("===================================")
    print("  COLETA DE DETALHES DE VOTAÇÕES")
    print("===================================")
    
    # Cria pasta de saída se não existir
    os.makedirs(PASTA_OUTPUT, exist_ok=True)
    
    total_geral = 0
    
    # Processa cada ano sequencialmente (a paralelização já ocorre dentro de cada ano)
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        try:
            itens_coletados = coletar_detalhes_ano(ano)
            total_geral += itens_coletados
        except KeyboardInterrupt:
            print(f"\nInterrompido pelo usuário. Dados parciais salvos.")
            break
        except Exception as e:
            print(f"Erro inesperado no ano {ano}: {e}")
            continue
    
    print("\n===================================")
    print("      COLETA CONCLUÍDA")
    print("===================================")
    print(f"Anos processados: {ANO_INICIO} a {ANO_FIM}")
    print(f"Total de votações detalhadas coletadas: {total_geral}")
    print(f"Arquivos salvos na pasta: {PASTA_OUTPUT}")
    print(f"Formato: votacoesID_XXXX.json")

if __name__ == "__main__":
    main()