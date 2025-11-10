import os
import json
import time
import random
import requests
import calendar
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse

# --- Configurações ---
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2/votacoes"
PASTA_DADOS = "../dados_coletados/dados_votacoes"
PASTA_DETALHES = "../dados_coletados/dados_detalhes"
ITENS_POR_PAGINA = 100
TEMPO_ESPERA_SEC = 0.5
TEMPO_ESPERA_RETRY_SEC = 10
MAX_TENTATIVAS = 5

os.makedirs(PASTA_DETALHES, exist_ok=True)


class DetalhesVotacoesCollector:
    """
    Coleta os endpoints de detalhes para cada votação já baixada:
      - /votacoes/{id}/votos
      - /votacoes/{id}/orientacoes

    O coletor procura por arquivos JSON em `dados_coletados/dados_votacoes/` com a lista
    de votações (por exemplo: `votacoes_2020.json`), extrai os ids e salva
    os dados obtidos em `dados_coletados/dados_detalhes/` como:
      - votos_{id}.json
      - orientacoes_{id}.json

    Uso (CLI):
      python detalhes_votacoes.py
    """

    def __init__(self,
                 base_url: str = BASE_URL,
                 pasta_dados: str = PASTA_DADOS,
                 pasta_saida: str = PASTA_DETALHES,
                 tempo_espera: float = TEMPO_ESPERA_SEC,
                 tempo_retry: float = TEMPO_ESPERA_RETRY_SEC,
                 max_tentativas: int = MAX_TENTATIVAS):
        self.base_url = base_url.rstrip('/')
        self.pasta_dados = pasta_dados
        self.pasta_saida = pasta_saida
        self.tempo_espera = tempo_espera
        self.tempo_retry = tempo_retry
        self.max_tentativas = max_tentativas

    def _read_ids_from_folder(self, anos: Optional[List[int]] = None) -> Dict[int, List[str]]:
        """
        Lê arquivos `votacoes_{ano}.json` em `pasta_dados` e retorna um dicionário
        {ano: [ids...]}. Se `anos` for None, tenta ler todos os arquivos `votacoes_*.json`.
        """
        ids_por_ano: Dict[int, List[str]] = {}
        try:
            for nome in os.listdir(self.pasta_dados):
                if nome.startswith('votacoes_') and nome.endswith('.json'):
                    # tenta extrair o ano do nome do arquivo
                    try:
                        partes = nome.replace('.json', '').split('_')
                        ano = int(partes[-1])
                    except Exception:
                        # pula arquivos com nome inesperado
                        continue

                    if anos is not None and ano not in anos:
                        continue

                    caminho = os.path.join(self.pasta_dados, nome)
                    try:
                        with open(caminho, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        idv = item.get('id')
                                        if idv is not None:
                                            ids_por_ano.setdefault(ano, []).append(str(idv))
                    except Exception as e:
                        print(f"Aviso: falha ao ler {caminho}: {e}")
        except FileNotFoundError:
            print(f"Pasta de dados não encontrada: {self.pasta_dados}")

        # remove duplicatas mantendo ordem por ano
        for ano, id_list in list(ids_por_ano.items()):
            seen = set()
            unique_ids = []
            for i in id_list:
                if i not in seen:
                    seen.add(i)
                    unique_ids.append(i)
            ids_por_ano[ano] = unique_ids

        return ids_por_ano

    def _request_with_retry(self, url, params=None):
        tentativa = 1
        while tentativa <= self.max_tentativas:
            try:
                resp = requests.get(url, params=params, headers={'Accept': 'application/json'})
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code in (500, 502, 503, 504):
                    print(f"Erro de servidor {resp.status_code} em {url}. Tentativa {tentativa}/{self.max_tentativas}.")
                    tentativa += 1
                    time.sleep(self.tempo_retry)
                    continue
                else:
                    print(f"Erro HTTP {resp.status_code} em {url}: {resp.text}")
                    return None
            except requests.RequestException as e:
                print(f"Erro de conexão ao acessar {url}: {e} (Tentativa {tentativa}/{self.max_tentativas})")
                tentativa += 1
                time.sleep(self.tempo_retry)
        print(f"Falha: excedeu {self.max_tentativas} tentativas para {url}")
        return None

    def coletar_para_id(self, id_votacao: str) -> bool:
        """Coleta e salva `votos` e `orientacoes` para um único id. Retorna True se algum dado foi salvo."""
        salvou_algum = False

        endpoints = {
            'votos': f"{self.base_url}/{id_votacao}/votos",
            'orientacoes': f"{self.base_url}/{id_votacao}/orientacoes"
        }

        for nome, url in endpoints.items():
            print(f"Coletando {nome} para id {id_votacao}...")
            # buscar todas as páginas do endpoint e agregar os 'dados'
            data = self._fetch_all_pages(url)
            if data is None:
                print(f"  Sem dados (erro) para {nome} do id {id_votacao}. Pulando.")
                continue

            # Se a API retornou sem 'dados', não salvamos um arquivo vazio
            dados_list = data.get('dados') if isinstance(data, dict) else None
            if not dados_list:
                print(f"  Retorno vazio para {nome} do id {id_votacao} (dados vazios). Pulando salvar.")
                continue

            # Salva em arquivo
            arquivo_saida = os.path.join(self.pasta_saida, f"{nome}_{id_votacao}.json")
            try:
                with open(arquivo_saida, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"  Salvo: {arquivo_saida}")
                salvou_algum = True
            except IOError as e:
                print(f"  Erro ao salvar {arquivo_saida}: {e}")

            time.sleep(self.tempo_espera)

        return salvou_algum

    def coletar_todos(self, ids: List[str] = None):
        """
        Coleta para todos os ids encontrados nos arquivos, ou para a lista `ids` fornecida.
        Retorna contagem de ids processados e de arquivos salvos.
        """
        # manter compatibilidade: se receber lista simples, processa sequencialmente
        if ids is not None:
            print(f"IDs a processar (lista direta): {len(ids)}")
            processados = 0
            salvos = 0
            for idv in ids:
                processados += 1
                ok = self.coletar_para_id(idv)
                if ok:
                    salvos += 1
            print(f"Processados: {processados} ids. Com dados salvos: {salvos}.")
            return processados, salvos

        # Se ids não for fornecido, lê por ano todos os arquivos
        ids_por_ano = self._read_ids_from_folder()
        total_processados = 0
        total_salvos = 0
        for ano in sorted(ids_por_ano.keys()):
            lista_ids = ids_por_ano[ano]
            print(f"\n--- Ano {ano}: {len(lista_ids)} IDs ---")
            p, s = self.coletar_por_ano(ano, lista_ids)
            total_processados += p
            total_salvos += s

        print(f"\nTotal processados: {total_processados}. Com dados salvos: {total_salvos}.")
        return total_processados, total_salvos

    def coletar_por_ano(self, ano: int, ids: Optional[List[str]] = None, max_workers: int = 10):
        """
        Coleta os detalhes para os ids de um ano usando ThreadPoolExecutor.
        Se `ids` for None, lê do arquivo `votacoes_{ano}.json`.
        Retorna (processados, salvos).
        """
        if ids is None:
            ids_por_ano = self._read_ids_from_folder([ano])
            ids = ids_por_ano.get(ano, [])

        total = len(ids)
        if total == 0:
            print(f"Nenhum id encontrado para o ano {ano}.")
            return 0, 0

        print(f"Iniciando coleta do ano {ano} com {total} ids usando {max_workers} threads...")
        lock = threading.Lock()
        processados = 0
        salvos = 0

        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            futures = {exe.submit(self.coletar_para_id, idv): idv for idv in ids}
            for fut in as_completed(futures):
                idv = futures[fut]
                try:
                    ok = fut.result()
                except Exception as e:
                    print(f"Erro ao processar id {idv}: {e}")
                    ok = False
                with lock:
                    processados += 1
                    if ok:
                        salvos += 1
                    if processados % 50 == 0 or processados == total:
                        print(f"  Progresso ano {ano}: {processados}/{total} (salvos: {salvos})")

        print(f"Concluído ano {ano}: processados {processados}, salvos {salvos}.")
        return processados, salvos

    def coletar_anos(self, anos: List[int], max_workers: int = 10):
        """Coleta sequencialmente por ano (cada ano com threads)."""
        total_p = 0
        total_s = 0
        for ano in sorted(anos):
            p, s = self.coletar_por_ano(ano, max_workers=max_workers)
            total_p += p
            total_s += s
        print(f"\nFinal: total processados {total_p}, total salvos {total_s}.")
        return total_p, total_s

    def _fetch_all_pages(self, url_base: str):
        """
        Busca todas as páginas de um recurso paginado da API da Câmara.
        Retorna um dicionário {'dados': [...], 'links': [...] } ou None em caso de erro.
        """
        # Primeiro tenta sem parâmetros (muitos endpoints não aceitam 'itens'/'pagina')
        agregados = []
        last_links = []

        resp_json = self._request_with_retry(url_base, params=None)
        if resp_json is None:
            return None

        dados = resp_json.get('dados', [])
        links = resp_json.get('links', [])
        last_links = links
        if dados:
            agregados.extend(dados)

        # Se a resposta indicar link 'next', segue usando os href fornecidos
        next_href = None
        for l in links:
            if l.get('rel') == 'next':
                next_href = l.get('href')
                break

        while next_href:
            time.sleep(self.tempo_espera)
            resp_json = self._request_with_retry(next_href, params=None)
            if resp_json is None:
                break
            dados = resp_json.get('dados', [])
            links = resp_json.get('links', [])
            last_links = links
            if dados:
                agregados.extend(dados)
            # procura próximo
            next_href = None
            for l in links:
                if l.get('rel') == 'next':
                    next_href = l.get('href')
                    break

        return {'dados': agregados, 'links': last_links}

    def _fetch_details_for_id(self, id_votacao: str):
        """Retorna uma tupla (votos_data_or_None, orientacoes_data_or_None)."""
        votos_url = f"{self.base_url}/{id_votacao}/votos"
        orient_url = f"{self.base_url}/{id_votacao}/orientacoes"
        votos = self._fetch_all_pages(votos_url)
        orient = self._fetch_all_pages(orient_url)
        return votos, orient

    def coletar_e_agregar_ano(self, ano: int, ids: Optional[List[str]] = None, max_workers: int = 10, orient_sample_size: int = 0, sample_votacoes: int = 0):
        """
        Coleta todos os detalhes dos ids do ano e agrega em dois arquivos:
          dados_coletados/dados_detalhes/votos/{ano}.json
          dados_coletados/dados_detalhes/orientacoes/{ano}.json

        Apenas salva entradas quando o 'dados' retornado não está vazio.
        Retorna (total_ids, arquivos_salvos)
        """
        if ids is None:
            ids_por_ano = self._read_ids_from_folder([ano])
            ids = ids_por_ano.get(ano, [])

        total = len(ids)
        if total == 0:
            print(f"Nenhum id para o ano {ano}. Pulando.")
            return 0, 0

        print(f"Agregando ano {ano}: {total} ids, threads={max_workers}")

        if sample_votacoes and isinstance(sample_votacoes, int) and sample_votacoes > 0:
            ids = ids[:sample_votacoes]
            total = len(ids)
            print(f"  Usando amostra de {total} votações (sample_votacoes={sample_votacoes})")

        votos_agg = {}
        orient_agg = {}
        lock = threading.Lock()

        def tarefa(idv):
            try:
                votos, orient = self._fetch_details_for_id(idv)
            except Exception as e:
                print(f"Erro fetch id {idv}: {e}")
                return
            with lock:
                # O novo formato é um dicionário {id_votacao: {dados...}}
                if isinstance(votos, dict) and votos.get('dados'):
                    votos_agg[idv] = votos

                if isinstance(orient, dict) and orient.get('dados'):
                    # A lógica de amostragem precisa ser tratada com cuidado aqui
                    # para não modificar o objeto original se não for desejado.
                    orient_list = orient.get('dados')
                    if orient_sample_size and isinstance(orient_sample_size, int) and orient_sample_size > 0:
                        if len(orient_list) > orient_sample_size:
                            # Cria uma cópia do objeto para não alterar o original
                            orient_copy = orient.copy()
                            orient_copy['dados'] = random.sample(orient_list, orient_sample_size)
                            orient_agg[idv] = orient_copy
                        else:
                            orient_agg[idv] = orient # usa o original se for menor que a amostra
                    else:
                        orient_agg[idv] = orient # sem amostragem, usa o original

        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            list(exe.map(tarefa, ids))

        is_sample_run = bool(sample_votacoes and sample_votacoes > 0) or bool(orient_sample_size and orient_sample_size > 0)

        # Define as pastas de saída baseando-se na amostragem
        if is_sample_run:
            base_output_dir = os.path.join(self.pasta_saida, 'amostra')
            print(f"  Execução em modo de amostragem. Saída será em: {base_output_dir}")
        else:
            base_output_dir = self.pasta_saida

        # Cria pastas de saída por endpoint
        votos_dir = os.path.join(base_output_dir, 'votos')
        orient_dir = os.path.join(base_output_dir, 'orientacoes')
        os.makedirs(votos_dir, exist_ok=True)
        os.makedirs(orient_dir, exist_ok=True)

        votos_file = os.path.join(votos_dir, f"{ano}.json")
        orient_file = os.path.join(orient_dir, f"{ano}.json")

        salvos = 0
        if votos_agg:
            try:
                # Escrever no formato similar a votacoes_20xx.json: uma lista de objetos, pretty-printed
                with open(votos_file, 'w', encoding='utf-8') as f:
                    json.dump(votos_agg, f, ensure_ascii=False, indent=4)
                print(f"Salvo votos agregados: {votos_file} ({len(votos_agg)} itens)")
                salvos += 1
            except Exception as e:
                print(f"Erro ao salvar {votos_file}: {e}")

        if orient_agg:
            try:
                # Escrever como lista de objetos, pretty-printed
                with open(orient_file, 'w', encoding='utf-8') as f:
                    json.dump(orient_agg, f, ensure_ascii=False, indent=4)
                print(f"Salvo orientacoes agregadas: {orient_file} ({len(orient_agg)} itens)")
                salvos += 1
            except Exception as e:
                print(f"Erro ao salvar {orient_file}: {e}")

        if salvos == 0:
            print(f"Nenhum dado salvo para o ano {ano} (todos vazios).")

        return total, salvos


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Coletor de detalhes de votações (votos e orientações)')
    parser.add_argument('--start', type=int, default=2018, help='Ano inicial (inclusive)')
    parser.add_argument('--end', type=int, default=2022, help='Ano final (inclusive)')
    parser.add_argument('--workers', type=int, default=10, help='Número de threads por ano')
    parser.add_argument('--dry-run', action='store_true', help='Apenas lista quantos IDs seriam processados, sem fazer requisições')
    parser.add_argument('--orient-sample-size', type=int, default=0, help='(Opcional) número máximo de orientações por votação a agregar (0 = todas)')
    parser.add_argument('--sample-votacoes', type=int, default=0, help='(Opcional) número de votações a processar por ano (0 = todas) — útil para demo')
    args = parser.parse_args()

    collector = DetalhesVotacoesCollector()
    anos = list(range(args.start, args.end + 1))

    # Dry-run: apenas contar e listar ids por ano
    if args.dry_run:
        ids_por_ano = collector._read_ids_from_folder(anos)
        for ano in sorted(anos):
            print(f"Ano {ano}: {len(ids_por_ano.get(ano, []))} IDs")
        total = sum(len(v) for v in ids_por_ano.values())
        print(f"Total IDs (anos {args.start}-{args.end}): {total}")
    else:
        # Executa por ano, cada ano com pool de threads, agregando por ano
        for ano in anos:
            collector.coletar_e_agregar_ano(ano, max_workers=args.workers, orient_sample_size=args.orient_sample_size, sample_votacoes=args.sample_votacoes)