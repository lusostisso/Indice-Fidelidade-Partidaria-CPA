import requests
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

# --- Configurações ---
BASE_URL_API = "https://dadosabertos.camara.leg.br/api/v2"
ANO_INICIO = 2018
ANO_FIM = 2022
PASTA_INPUT = "dados_votacoes"
PASTA_OUTPUT = "dados_votacoes"
TEMPO_ESPERA_SEC = 0.4        # Espera normal entre requisições
TEMPO_ESPERA_RETRY_SEC = 8    # Espera após erro
MAX_TENTATIVAS = 4            # Máximo de tentativas por requisição
MAX_WORKERS = 6               # Número de threads para paralelização

# Lock para thread-safe writing
write_lock = threading.Lock()
progress_lock = threading.Lock()

# Contadores globais
total_processados = 0
total_erros = 0
total_temas_encontrados = 0

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
                # 404 é esperado para algumas proposições que não existem mais
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

def processar_proposicao_temas(proposicao_id, ano):
    """
    Processa uma proposição específica, coletando seus temas e informações básicas.
    """
    global total_processados, total_erros, total_temas_encontrados
    
    try:
        # 1. Buscar temas da proposição
        url_temas = f"{BASE_URL_API}/proposicoes/{proposicao_id}/temas"
        dados_temas = fazer_requisicao_com_retry(url_temas, f"Temas proposição {proposicao_id}")
        
        # 2. Buscar detalhes básicos da proposição
        url_proposicao = f"{BASE_URL_API}/proposicoes/{proposicao_id}"
        dados_proposicao = fazer_requisicao_com_retry(url_proposicao, f"Proposição {proposicao_id}")
        
        # Inicializar o objeto resultado
        resultado = {
            "id": proposicao_id,
            "temas": [],
            "informacoes": {},
            "dataColeta": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Processar temas
        if dados_temas and 'dados' in dados_temas:
            temas = dados_temas.get('dados', [])
            resultado["temas"] = temas
            if temas:
                with progress_lock:
                    total_temas_encontrados += len(temas)
        
        # Processar informações da proposição
        if dados_proposicao and 'dados' in dados_proposicao:
            prop_dados = dados_proposicao.get('dados', {})
            resultado["informacoes"] = {
                'siglaTipo': prop_dados.get('siglaTipo'),
                'numero': prop_dados.get('numero'),
                'ano': prop_dados.get('ano'),
                'ementa': prop_dados.get('ementa'),
                'descricaoTipo': prop_dados.get('descricaoTipo'),
                'dataApresentacao': prop_dados.get('dataApresentacao'),
                'situacao': prop_dados.get('statusProposicao', {}).get('descricaoSituacao') if prop_dados.get('statusProposicao') else None,
                'uri': prop_dados.get('uri')
            }
        
        with progress_lock:
            total_processados += 1
            if total_processados % 25 == 0:
                print(f"      Processados: {total_processados} | Erros: {total_erros} | Temas encontrados: {total_temas_encontrados}")
        
        # Pequena pausa para evitar rate limiting
        time.sleep(TEMPO_ESPERA_SEC)
        
        return proposicao_id, resultado
        
    except Exception as e:
        print(f"      Erro inesperado ao processar proposição {proposicao_id}: {e}")
        with progress_lock:
            total_erros += 1
        return None, None

def extrair_proposicoes_de_votacoes(arquivo_votacoes):
    """
    Extrai todos os IDs únicos de proposições do campo 'proposicoesAfetadas' de um arquivo de votações.
    """
    proposicoes_ids = set()
    
    try:
        with open(arquivo_votacoes, 'r', encoding='utf-8') as f:
            votacoes = json.load(f)
        
        for votacao in votacoes:
            # Verificar se é um objeto válido com proposicoesAfetadas
            if isinstance(votacao, dict) and 'proposicoesAfetadas' in votacao:
                proposicoes_afetadas = votacao.get('proposicoesAfetadas', [])
                
                for proposicao in proposicoes_afetadas:
                    if isinstance(proposicao, dict):
                        prop_id = proposicao.get('id')
                        if prop_id:
                            proposicoes_ids.add(str(prop_id))
        
        return list(proposicoes_ids)
        
    except Exception as e:
        print(f"Erro ao processar arquivo {arquivo_votacoes}: {e}")
        return []

def coletar_temas_ano(ano):
    """
    Coleta temas das proposições para um ano específico.
    """
    global total_processados, total_erros, total_temas_encontrados
    
    print(f"\n--- Iniciando coleta de temas para o ano: {ano} ---")
    
    # Carrega o arquivo de votações detalhadas do ano
    arquivo_votacoes = os.path.join(PASTA_INPUT, f"votacoesID_{ano}.json")
    
    if not os.path.exists(arquivo_votacoes):
        print(f"Arquivo {arquivo_votacoes} não encontrado. Pulando ano {ano}.")
        return 0
    
    # Extrair IDs únicos de proposições
    print("Extraindo IDs de proposições do arquivo...")
    proposicoes_ids = extrair_proposicoes_de_votacoes(arquivo_votacoes)
    
    if not proposicoes_ids:
        print(f"Nenhuma proposição encontrada para o ano {ano}")
        return 0
    
    print(f"Encontradas {len(proposicoes_ids)} proposições únicas para processar")
    
    # Reset contadores para este ano
    total_processados = 0
    total_erros = 0
    total_temas_encontrados = 0
    
    # Processar proposições em paralelo
    proposicoes_com_temas = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submete todas as tarefas
        future_to_proposicao = {
            executor.submit(processar_proposicao_temas, prop_id, ano): prop_id 
            for prop_id in proposicoes_ids
        }
        
        # Coleta os resultados conforme completam
        for future in as_completed(future_to_proposicao):
            prop_id, dados_temas = future.result()
            if dados_temas:
                proposicoes_com_temas[prop_id] = dados_temas
    
    # Salva os dados coletados
    if proposicoes_com_temas:
        # Converte para lista ordenada por ID para facilitar consulta
        lista_proposicoes = []
        for prop_id in sorted(proposicoes_com_temas.keys(), key=int):
            lista_proposicoes.append(proposicoes_com_temas[prop_id])
        
        arquivo_saida = os.path.join(PASTA_OUTPUT, f"proposicaoTema_{ano}.json")
        try:
            with open(arquivo_saida, 'w', encoding='utf-8') as f:
                json.dump(lista_proposicoes, f, ensure_ascii=False, indent=2)
            
            print(f"\n--- DADOS SALVOS: {arquivo_saida} ---")
            print(f"    Total de proposições processadas: {len(lista_proposicoes)}")
            print(f"    Total de proposições com temas: {len([p for p in lista_proposicoes if p.get('temas')])}")
            print(f"    Total de temas coletados: {total_temas_encontrados}")
            print(f"    Total com erro: {total_erros}")
            return len(lista_proposicoes)
            
        except Exception as e:
            print(f"Erro ao salvar arquivo {arquivo_saida}: {e}")
            return 0
    else:
        print(f"Nenhum dado coletado para o ano {ano}")
        return 0

def criar_relatorio_resumo():
    """
    Cria um relatório resumo de todos os temas coletados.
    """
    print("\n--- Gerando relatório resumo ---")
    
    resumo = {
        "dataGeracao": time.strftime("%Y-%m-%d %H:%M:%S"),
        "totalProposicoesPorAno": {},
        "totalTemasPorAno": {},
        "temasUnicos": set(),
        "estatisticas": {}
    }
    
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        arquivo_temas = os.path.join(PASTA_OUTPUT, f"proposicaoTema_{ano}.json")
        
        if os.path.exists(arquivo_temas):
            try:
                with open(arquivo_temas, 'r', encoding='utf-8') as f:
                    proposicoes = json.load(f)
                
                proposicoes_com_temas = 0
                total_temas = 0
                
                for prop in proposicoes:
                    temas = prop.get('temas', [])
                    if temas:
                        proposicoes_com_temas += 1
                        total_temas += len(temas)
                        
                        # Coletar temas únicos
                        for tema in temas:
                            if isinstance(tema, dict) and 'tema' in tema:
                                resumo["temasUnicos"].add(tema['tema'])
                
                resumo["totalProposicoesPorAno"][ano] = len(proposicoes)
                resumo["totalTemasPorAno"][ano] = total_temas
                
            except Exception as e:
                print(f"Erro ao processar arquivo do ano {ano}: {e}")
    
    # Converter set para lista para serialização JSON
    resumo["temasUnicos"] = sorted(list(resumo["temasUnicos"]))
    
    # Calcular estatísticas gerais
    resumo["estatisticas"] = {
        "totalProposicoes": sum(resumo["totalProposicoesPorAno"].values()),
        "totalTemas": sum(resumo["totalTemasPorAno"].values()),
        "totalTemasUnicos": len(resumo["temasUnicos"]),
        "anosMaiorColeta": max(resumo["totalTemasPorAno"], key=resumo["totalTemasPorAno"].get) if resumo["totalTemasPorAno"] else None
    }
    
    # Salvar relatório
    arquivo_relatorio = os.path.join(PASTA_OUTPUT, "relatorio_temas_proposicoes.json")
    try:
        with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
            json.dump(resumo, f, ensure_ascii=False, indent=2)
        
        print(f"Relatório salvo em: {arquivo_relatorio}")
        print(f"Total de proposições processadas: {resumo['estatisticas']['totalProposicoes']}")
        print(f"Total de temas coletados: {resumo['estatisticas']['totalTemas']}")
        print(f"Temas únicos encontrados: {resumo['estatisticas']['totalTemasUnicos']}")
        
    except Exception as e:
        print(f"Erro ao salvar relatório: {e}")

def main():
    """
    Função principal que coordena a coleta para todos os anos.
    """
    print("=" * 60)
    print("  COLETA DE TEMAS DAS PROPOSIÇÕES")
    print("=" * 60)
    print("Esta coleta vai:")
    print("1. Ler os arquivos votacoesID_XXXX.json")
    print("2. Extrair IDs do campo 'proposicoesAfetadas'")
    print("3. Buscar temas via proposicoes/{id}/temas")
    print("4. Salvar em proposicaoTema_XXXX.json")
    print("=" * 60)
    
    # Cria pasta de saída se não existir
    os.makedirs(PASTA_OUTPUT, exist_ok=True)
    
    total_geral = 0
    
    # Processa cada ano sequencialmente
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        try:
            itens_coletados = coletar_temas_ano(ano)
            total_geral += itens_coletados
        except KeyboardInterrupt:
            print(f"\nInterrompido pelo usuário. Dados parciais salvos.")
            break
        except Exception as e:
            print(f"Erro inesperado no ano {ano}: {e}")
            continue
    
    print("\n" + "=" * 60)
    print("      COLETA CONCLUÍDA")
    print("=" * 60)
    print(f"Anos processados: {ANO_INICIO} a {ANO_FIM}")
    print(f"Total de proposições processadas: {total_geral}")
    print(f"Arquivos gerados: proposicaoTema_XXXX.json")
    print(f"Pasta: {PASTA_OUTPUT}")
    
    # Gerar relatório resumo
    criar_relatorio_resumo()

if __name__ == "__main__":
    main()