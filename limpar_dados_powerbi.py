"""
Script para limpar e consolidar dados de votações para Power BI.

Este script:
1. Carrega todos os arquivos de dados por ano
2. Junta as informações usando o ID de votação como chave
3. Filtra votações sem orientações ou sem votos
4. Gera um arquivo CSV limpo para Power BI

IMPORTANTE: O script trata IDs de votação em dois formatos:
- Formato completo: "2152544-73" (com sufixo)
- Formato base: "2152544" (sem sufixo)
Ambos representam a mesma votação e são linkados corretamente.
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd
from datetime import datetime

# Configurações
PASTA_DADOS = "dados_coletados"
ANOS = [2018, 2019, 2020, 2021, 2022]
ARQUIVO_SAIDA_VOTACOES = "dados_limpos_powerbi.csv"
ARQUIVO_SAIDA_VOTOS = "votos_deputados_powerbi.csv"
ARQUIVO_PARTIDOS = os.path.join(PASTA_DADOS, "dados_partidos", "partidos_existentes.csv")


def normalizar_id_votacao(votacao_id: str) -> str:
    """
    Normaliza o ID de votação para extrair o ID base.
    
    Exemplos:
    - "2152544-73" -> "2152544"
    - "2152544" -> "2152544"
    - "2168389-2" -> "2168389"
    """
    if not votacao_id:
        return ""
    # Remove o sufixo após o hífen se existir
    return votacao_id.split('-')[0]


def buscar_id_equivalente(votacao_id: str, dicionario: Dict) -> Optional[any]:
    """
    Busca um ID em um dicionário considerando tanto o formato completo quanto o formato base.
    
    Primeiro tenta buscar pelo ID exato, depois pelo ID normalizado (base).
    """
    if not votacao_id:
        return None
    
    # Tentar busca exata primeiro
    if votacao_id in dicionario:
        return dicionario[votacao_id]
    
    # Tentar busca pelo ID base (sem sufixo)
    id_base = normalizar_id_votacao(votacao_id)
    if id_base and id_base != votacao_id:
        if id_base in dicionario:
            return dicionario[id_base]
    
    return None


def extrair_id_da_uri(uri: Optional[str]) -> Optional[str]:
    """
    Extrai o ID numérico de uma URI da API.
    Exemplo: 'https://dadosabertos.camara.leg.br/api/v2/proposicoes/2270857' -> '2270857'
    """
    if not uri:
        return None
    match = re.search(r"/(\\d+)/?$", uri)
    if match:
        return match.group(1)
    return None


def carregar_json(arquivo: str) -> Optional[any]:
    """Carrega um arquivo JSON."""
    try:
        with open(arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {arquivo}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON {arquivo}: {e}")
        return None


def carregar_votacoes(ano: int) -> Dict[str, dict]:
    """
    Carrega as votações básicas de um ano.
    Cria índice tanto pelo ID completo quanto pelo ID base (sem sufixo).
    """
    arquivo = os.path.join(PASTA_DADOS, "dados_votacoes", f"votacoes_{ano}.json")
    dados = carregar_json(arquivo)
    if not dados:
        return {}
    
    # Criar dicionário indexado por ID
    votacoes = {}
    for votacao in dados:
        vote_id = votacao.get("id")
        if vote_id:
            # Indexar pelo ID completo
            votacoes[vote_id] = votacao
            # Também indexar pelo ID base (sem sufixo) se for diferente
            id_base = normalizar_id_votacao(vote_id)
            if id_base and id_base != vote_id and id_base not in votacoes:
                votacoes[id_base] = votacao
    return votacoes


def carregar_votacoes_detalhes(ano: int) -> Dict[str, dict]:
    """
    Carrega os detalhes das votações de um ano.
    Cria índice tanto pelo ID completo quanto pelo ID base (sem sufixo).
    """
    arquivo = os.path.join(PASTA_DADOS, "dados_votacoes", f"votacoesID_{ano}.json")
    dados = carregar_json(arquivo)
    if not dados:
        return {}
    
    # Criar dicionário indexado por ID
    detalhes = {}
    for votacao in dados:
        vote_id = votacao.get("id")
        if vote_id:
            # Indexar pelo ID completo
            detalhes[vote_id] = votacao
            # Também indexar pelo ID base (sem sufixo) se for diferente
            id_base = normalizar_id_votacao(vote_id)
            if id_base and id_base != vote_id and id_base not in detalhes:
                detalhes[id_base] = votacao
    return detalhes


def carregar_temas_proposicoes(ano: int) -> Dict[str, dict]:
    """Carrega os temas e informações das proposições de um ano."""
    arquivo = os.path.join(PASTA_DADOS, "dados_votacoes", f"proposicaoTema_{ano}.json")
    dados = carregar_json(arquivo)
    if not dados:
        return {}
    
    # Criar dicionário indexado por ID da proposição
    temas_dict: Dict[str, dict] = {}
    for item in dados:
        prop_id = str(item.get("id"))
        if not prop_id:
            continue
        temas = item.get("temas", []) or []
        info = item.get("informacoes", {}) or {}
        temas_dict[prop_id] = {
            "id": prop_id,
            "temas": temas,
            "siglaTipo": info.get("siglaTipo"),
            "numero": info.get("numero"),
            "ano": info.get("ano"),
            "situacao": info.get("situacao"),
            "ementa": info.get("ementa"),
        }
    return temas_dict


def carregar_votos(ano: int) -> Dict[str, List[dict]]:
    """
    Carrega os votos dos deputados de um ano.
    Cria índice tanto pelo ID completo quanto pelo ID base (sem sufixo).
    """
    arquivo = os.path.join(PASTA_DADOS, "dados_detalhes", "votos", f"{ano}.json")
    dados = carregar_json(arquivo)
    if not dados:
        return {}
    
    # O arquivo tem estrutura: { "vote_id": { "dados": [...] } }
    votos_dict = {}
    for vote_id, conteudo in dados.items():
        votos_list = conteudo.get("dados", [])
        if votos_list:
            # Indexar pelo ID completo
            votos_dict[vote_id] = votos_list
            # Também indexar pelo ID base (sem sufixo) se for diferente
            id_base = normalizar_id_votacao(vote_id)
            if id_base and id_base != vote_id and id_base not in votos_dict:
                votos_dict[id_base] = votos_list
    return votos_dict


def carregar_orientacoes(ano: int) -> Dict[str, List[dict]]:
    """
    Carrega as orientações dos partidos de um ano.
    Cria índice tanto pelo ID completo quanto pelo ID base (sem sufixo).
    """
    arquivo = os.path.join(PASTA_DADOS, "dados_detalhes", "orientacoes", f"{ano}.json")
    dados = carregar_json(arquivo)
    if not dados:
        return {}
    
    # O arquivo tem estrutura: { "vote_id": { "dados": [...] } }
    orientacoes_dict = {}
    for vote_id, conteudo in dados.items():
        orientacoes_list = conteudo.get("dados", [])
        if orientacoes_list:
            # Indexar pelo ID completo
            orientacoes_dict[vote_id] = orientacoes_list
            # Também indexar pelo ID base (sem sufixo) se for diferente
            id_base = normalizar_id_votacao(vote_id)
            if id_base and id_base != vote_id and id_base not in orientacoes_dict:
                orientacoes_dict[id_base] = orientacoes_list
    return orientacoes_dict


def extrair_ids_proposicoes_relacionadas(votacao_detalhes: dict) -> List[str]:
    """
    Extrai IDs de proposições relacionadas a uma votação a partir de diversos campos
    presentes em `votacoesID_XXXX.json`.
    """
    ids: List[str] = []
    vistos: Set[str] = set()

    def add_id(raw_id: Optional[object]):
        if not raw_id:
            return
        pid = str(raw_id)
        if pid not in vistos:
            vistos.add(pid)
            ids.append(pid)

    # proposicoesAfetadas (lista de objetos com id/uri)
    for prop in votacao_detalhes.get("proposicoesAfetadas", []) or []:
        if isinstance(prop, dict):
            add_id(prop.get("id"))
            if not prop.get("id"):
                add_id(extrair_id_da_uri(prop.get("uri")))

    # objetosPossiveis
    for obj in votacao_detalhes.get("objetosPossiveis", []) or []:
        if isinstance(obj, dict):
            add_id(obj.get("id"))
            if not obj.get("id"):
                add_id(extrair_id_da_uri(obj.get("uri")))

    # proposicoesRelacionadas
    for prop in votacao_detalhes.get("proposicoesRelacionadas", []) or []:
        if isinstance(prop, dict):
            add_id(prop.get("id"))
            if not prop.get("id"):
                add_id(extrair_id_da_uri(prop.get("uri")))

    # temasProposicoes (chaves são IDs de proposição)
    temas_props = votacao_detalhes.get("temasProposicoes") or {}
    if isinstance(temas_props, dict):
        for key in temas_props.keys():
            add_id(key)

    # ultimaApresentacaoProposicao.uriProposicaoCitada
    ultima = votacao_detalhes.get("ultimaApresentacaoProposicao") or {}
    if isinstance(ultima, dict):
        add_id(extrair_id_da_uri(ultima.get("uriProposicaoCitada")))

    return ids


def extrair_temas_da_votacao(
    votacao_detalhes: dict, temas_proposicoes: Dict[str, dict]
) -> Tuple[List[str], Optional[dict]]:
    """
    Extrai os temas e a proposição principal relacionada à votação.

    Retorna:
      - lista de nomes de temas
      - dicionário com informações da primeira proposição encontrada (ou None)
    """
    temas_encontrados: List[str] = []
    proposicao_escolhida: Optional[dict] = None

    ids_props = extrair_ids_proposicoes_relacionadas(votacao_detalhes)

    for prop_id in ids_props:
        dados_prop = temas_proposicoes.get(str(prop_id))
        if not dados_prop:
            continue

        # Temas desta proposição
        for tema in dados_prop.get("temas", []) or []:
            if isinstance(tema, dict):
                tema_nome = tema.get("tema")
            else:
                tema_nome = None
            if tema_nome and tema_nome not in temas_encontrados:
                temas_encontrados.append(tema_nome)

        # Definir proposição principal se ainda não houver
        if proposicao_escolhida is None:
            proposicao_escolhida = dict(dados_prop)
            proposicao_escolhida["id"] = prop_id

    return temas_encontrados, proposicao_escolhida


def obter_orientacao_partido(sigla_partido: str, orientacoes: List[dict]) -> Optional[str]:
    """
    Obtém a orientação de voto de um partido específico.
    
    Retorna a orientação do partido ou None se não houver orientação específica.
    Prioriza orientações diretas do partido sobre orientações de blocos.
    """
    if not orientacoes or not sigla_partido:
        return None
    
    sigla_partido_upper = sigla_partido.upper()
    orientacao_bloco = None  # Para armazenar orientação de bloco como fallback
    
    # Primeiro, buscar orientação específica do partido (prioridade)
    for orientacao in orientacoes:
        sigla_partido_bloco = orientacao.get("siglaPartidoBloco", "")
        cod_tipo_lideranca = orientacao.get("codTipoLideranca", "")
        
        # Se for orientação de partido (não bloco) e a sigla corresponder exatamente
        if cod_tipo_lideranca == "P":
            sigla_bloco_upper = sigla_partido_bloco.upper()
            if sigla_bloco_upper == sigla_partido_upper:
                return orientacao.get("orientacaoVoto")
        
        # Se for bloco, verificar se o partido está no bloco (fallback)
        elif cod_tipo_lideranca == "B":
            sigla_bloco_upper = sigla_partido_bloco.upper()
            # Verificar se a sigla do partido está no nome do bloco
            # Exemplo: "PsdbPsdPrPrb..." contém "PSDB", "PSD", "PR", "PRB"
            if sigla_partido_upper in sigla_bloco_upper:
                # Armazenar como fallback (pode haver múltiplos blocos)
                if orientacao_bloco is None:
                    orientacao_bloco = orientacao.get("orientacaoVoto")
    
    # Retornar orientação de bloco se não houver orientação direta do partido
    return orientacao_bloco


def verificar_fidelidade_partidaria(voto_deputado: str, orientacao_partido: Optional[str]) -> Optional[bool]:
    """
    Verifica se o voto do deputado está alinhado com a orientação do partido.
    
    Retorna:
    - True: se o deputado seguiu a orientação
    - False: se o deputado não seguiu a orientação
    - None: se não há orientação do partido ou voto inválido
    """
    if not orientacao_partido or not voto_deputado:
        return None
    
    # Mapear votos para valores comparáveis
    voto_normalizado = voto_deputado.strip()
    orientacao_normalizada = orientacao_partido.strip()
    
    # Se a orientação for "Liberada", não há fidelidade a verificar
    if orientacao_normalizada == "Liberada":
        return None
    
    # Comparar diretamente
    return voto_normalizado == orientacao_normalizada


def validar_votacao(votacao_id: str, votos: List[dict], orientacoes: List[dict]) -> bool:
    """
    Valida se uma votação tem votos registrados.
    
    Agora, uma votação é considerada válida SE tiver votos, independentemente de
    existir ou não orientação explícita de partido.
    """
    tem_votos = votos and len(votos) > 0
    return tem_votos


def processar_votacao(votacao_id: str, 
                     votacao_basica: dict,
                     votacao_detalhes: dict,
                     votos: List[dict],
                     orientacoes: List[dict],
                     temas_proposicoes: Dict[str, dict]) -> Optional[dict]:
    """Processa uma votação e retorna um dicionário com os dados limpos."""
    
    # Validar votação
    if not validar_votacao(votacao_id, votos, orientacoes):
        return None
    
    # Extrair temas e proposição
    temas, proposicao_info = extrair_temas_da_votacao(votacao_detalhes, temas_proposicoes)
    
    # Extrair informações básicas
    dados_limpos = {
        "id_votacao": votacao_id,
        "data": votacao_basica.get("data"),
        "dataHoraRegistro": votacao_basica.get("dataHoraRegistro"),
        "siglaOrgao": votacao_basica.get("siglaOrgao"),
        "aprovacao": votacao_basica.get("aprovacao"),
        "proposicaoObjeto": votacao_basica.get("proposicaoObjeto"),
        
        # Informações dos detalhes
        "idOrgao": votacao_detalhes.get("idOrgao"),
        "idEvento": votacao_detalhes.get("idEvento"),
        "descUltimaAberturaVotacao": votacao_detalhes.get("descUltimaAberturaVotacao"),
        "dataHoraUltimaAberturaVotacao": votacao_detalhes.get("dataHoraUltimaAberturaVotacao"),
        
        # Proposição principal (se houver)
        "proposicaoId": None,
        "proposicaoAno": None,
        "proposicaoSituacao": None,
        "proposicaoEmenta": None,
        
        # Temas (concatenados)
        "temas": "; ".join(temas) if temas else None,
        "quantidade_temas": len(temas),
        
        # Estatísticas de votos
        "total_votos": len(votos) if votos else 0,
        "votos_sim": sum(1 for v in votos if v.get("tipoVoto") == "Sim") if votos else 0,
        "votos_nao": sum(1 for v in votos if v.get("tipoVoto") == "Não") if votos else 0,
        "votos_abstencao": sum(1 for v in votos if v.get("tipoVoto") == "Abstenção") if votos else 0,
        "votos_obstrucao": sum(1 for v in votos if v.get("tipoVoto") == "Obstrução") if votos else 0,
        "votos_outros": sum(1 for v in votos if v.get("tipoVoto") not in ["Sim", "Não", "Abstenção", "Obstrução"]) if votos else 0,
        
        # Estatísticas de orientações
        "total_orientacoes": len(orientacoes) if orientacoes else 0,
        "orientacoes_sim": sum(1 for o in orientacoes if o.get("orientacaoVoto") == "Sim") if orientacoes else 0,
        "orientacoes_nao": sum(1 for o in orientacoes if o.get("orientacaoVoto") == "Não") if orientacoes else 0,
        "orientacoes_abstencao": sum(1 for o in orientacoes if o.get("orientacaoVoto") == "Abstenção") if orientacoes else 0,
        "orientacoes_liberada": sum(1 for o in orientacoes if o.get("orientacaoVoto") == "Liberada") if orientacoes else 0,
        "orientacoes_outras": sum(1 for o in orientacoes if o.get("orientacaoVoto") not in ["Sim", "Não", "Abstenção", "Liberada"]) if orientacoes else 0,
    }
    
    # Preencher dados da proposição, se disponíveis
    if proposicao_info:
        dados_limpos["proposicaoId"] = proposicao_info.get("id")
        dados_limpos["proposicaoAno"] = proposicao_info.get("ano")
        dados_limpos["proposicaoSituacao"] = proposicao_info.get("situacao")
        dados_limpos["proposicaoEmenta"] = proposicao_info.get("ementa")
    
    return dados_limpos


def processar_votos_deputados(votacao_id: str,
                              votacao_basica: dict,
                              votacao_detalhes: dict,
                              votos: List[dict],
                              orientacoes: List[dict],
                              temas_proposicoes: Dict[str, dict]) -> List[dict]:
    """
    Processa os votos individuais dos deputados para uma votação.
    Retorna uma lista de dicionários, um para cada voto de deputado.
    """
    votos_deputados = []
    
    if not votos:
        return votos_deputados
    
    # Extrair temas e proposição
    temas, proposicao_info = extrair_temas_da_votacao(votacao_detalhes, temas_proposicoes)
    
    # Informações básicas da votação
    info_votacao = {
        "id_votacao": votacao_id,
        "data": votacao_basica.get("data"),
        "siglaOrgao": votacao_basica.get("siglaOrgao"),
        "aprovacao": votacao_basica.get("aprovacao"),
        "temas": "; ".join(temas) if temas else None,
        "proposicaoId": None,
        "proposicaoAno": None,
        "proposicaoSituacao": None,
    }
    
    # Preencher dados da proposição, se disponíveis
    if proposicao_info:
        info_votacao["proposicaoId"] = proposicao_info.get("id")
        info_votacao["proposicaoAno"] = proposicao_info.get("ano")
        info_votacao["proposicaoSituacao"] = proposicao_info.get("situacao")
    
    # Processar cada voto
    for voto in votos:
        deputado_info = voto.get("deputado_", {})
        if not deputado_info:
            continue
        
        tipo_voto = voto.get("tipoVoto")
        sigla_partido = deputado_info.get("siglaPartido")
        
        # Obter orientação do partido
        orientacao_partido = obter_orientacao_partido(sigla_partido, orientacoes) if orientacoes else None
        
        # Verificar fidelidade partidária
        fidelidade = verificar_fidelidade_partidaria(tipo_voto, orientacao_partido)
        
        # Criar registro do voto do deputado
        voto_deputado = {
            # Informações da votação
            **info_votacao,
            
            # Informações do deputado
            "deputado_id": deputado_info.get("id"),
            "deputado_nome": deputado_info.get("nome"),
            "deputado_partido": sigla_partido,
            "deputado_uf": deputado_info.get("siglaUf"),
            "deputado_legislatura": deputado_info.get("idLegislatura"),
            "deputado_email": deputado_info.get("email"),
            
            # Informações do voto
            "voto": tipo_voto,
            "data_registro_voto": voto.get("dataRegistroVoto"),
            
            # Informações da orientação do partido
            "orientacao_partido": orientacao_partido,
            "fidelidade_partidaria": fidelidade,
        }
        
        votos_deputados.append(voto_deputado)
    
    return votos_deputados


def processar_ano(ano: int) -> Tuple[List[dict], List[dict]]:
    """
    Processa todos os dados de um ano.
    Retorna uma tupla: (dados_votacoes, dados_votos_deputados)
    """
    print(f"\nProcessando ano {ano}...")
    
    # Carregar todos os dados
    votacoes_basicas = carregar_votacoes(ano)
    votacoes_detalhes = carregar_votacoes_detalhes(ano)
    temas_proposicoes = carregar_temas_proposicoes(ano)
    votos = carregar_votos(ano)
    orientacoes = carregar_orientacoes(ano)
    
    print(f"  Votações básicas: {len(votacoes_basicas)}")
    print(f"  Votos: {len(votos)}")
    
    dados_limpos = []
    votos_deputados_lista = []
    votacoes_validas = 0
    votacoes_invalidas = 0
    total_votos_processados = 0

    # Estratégia corrigida: Iterar pelos IDs que TEM VOTOS primeiro.
    # Se o objetivo é PowerBI de votos, o que dita a regra é o arquivo de votos.
    ids_para_processar = list(votos.keys())
    
    # Adicionar IDs das votações básicas que não estão nos votos 
    # (apenas para contabilizar as 'inválidas' corretamente no log, se quiser manter a estatística)
    ids_basicos = set(votacoes_basicas.keys())
    ids_apenas_basicos = ids_basicos - set(ids_para_processar)
    ids_para_processar.extend(list(ids_apenas_basicos))

    # Set para evitar duplicatas exatas de ID (caso o json tenha sujeira)
    ids_ja_processados_exatos = set()

    for votacao_id in ids_para_processar:
        if votacao_id in ids_ja_processados_exatos:
            continue
        ids_ja_processados_exatos.add(votacao_id)
        
        # Tenta pegar os dados básicos.
        # Se não achar pelo ID completo, tenta pelo ID base (normalizado)
        votacao_basica = votacoes_basicas.get(votacao_id)
        if not votacao_basica:
            votacao_basica = buscar_id_equivalente(votacao_id, votacoes_basicas)
        
        # Se mesmo assim não achar dados básicos, cria um stub se houver votos
        # Isso garante que não perdemos os votos só porque faltou metadado da votação
        if not votacao_basica:
             votacao_basica = {"id": votacao_id, "data": None, "siglaOrgao": "CAMARA"}

        # Busca os componentes usando o ID exato ou equivalente
        votacao_detalhe = votacoes_detalhes.get(votacao_id) or buscar_id_equivalente(votacao_id, votacoes_detalhes) or {}
        votos_votacao = votos.get(votacao_id) or buscar_id_equivalente(votacao_id, votos) or []
        orientacoes_votacao = orientacoes.get(votacao_id) or buscar_id_equivalente(votacao_id, orientacoes) or []
        
        # Processar
        dados = processar_votacao(
            votacao_id, # Usa o ID original específico (com sufixo)
            votacao_basica,
            votacao_detalhe,
            votos_votacao,
            orientacoes_votacao,
            temas_proposicoes
        )
        
        if dados:
            dados_limpos.append(dados)
            votacoes_validas += 1
            
            votos_dep = processar_votos_deputados(
                votacao_id,
                votacao_basica,
                votacao_detalhe,
                votos_votacao,
                orientacoes_votacao,
                temas_proposicoes
            )
            votos_deputados_lista.extend(votos_dep)
            total_votos_processados += len(votos_dep)
        else:
            votacoes_invalidas += 1
    
    print(f"  Votações válidas: {votacoes_validas}")
    print(f"  Votações sem votos nominais: {votacoes_invalidas}")
    print(f"  Votos individuais processados: {total_votos_processados}")
    
    return dados_limpos, votos_deputados_lista


def main():
    """Função principal."""
    print("=" * 60)
    print("Limpeza e Consolidação de Dados para Power BI")
    print("=" * 60)
    
    # Verificar se a pasta existe
    if not os.path.exists(PASTA_DADOS):
        print(f"ERRO: Pasta '{PASTA_DADOS}' não encontrada!")
        return
    
    # Processar todos os anos
    todos_dados_votacoes = []  # ainda usado para estatísticas internas (não geramos CSV)
    todos_dados_votos = []
    
    for ano in ANOS:
        try:
            dados_votacoes, dados_votos = processar_ano(ano)
            todos_dados_votacoes.extend(dados_votacoes)
            todos_dados_votos.extend(dados_votos)
        except Exception as e:
            print(f"ERRO ao processar ano {ano}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if len(todos_dados_votacoes) == 0:
        print("\nERRO: Nenhum dado válido foi encontrado!")
        return
    
    print(f"\n{'=' * 60}")
    print(f"Total de votações válidas: {len(todos_dados_votacoes)}")
    print(f"Total de votos individuais: {len(todos_dados_votos):,}")
    print(f"{'=' * 60}")
    
    # Criar DataFrame principal de votos individuais
    try:
        df_votos = pd.DataFrame(todos_dados_votos)
        df_votos['data'] = pd.to_datetime(df_votos['data'], errors='coerce')
        df_votos = df_votos.sort_values(['data', 'deputado_nome'])

        # Adicionar ideologia dos partidos (Task 3)
        ideologia_map = {}
        try:
            if os.path.exists(ARQUIVO_PARTIDOS):
                df_partidos = pd.read_csv(ARQUIVO_PARTIDOS, encoding="utf-8")
                # Criar mapa SIGLA -> IDEOLOGIA (normalizando para maiúsculas)
                for _, row in df_partidos.iterrows():
                    sigla = str(row.get("SIGLA", "")).strip().upper()
                    ideologia = str(row.get(" IDEOLOGIA", row.get("IDEOLOGIA", ""))).strip()
                    if sigla:
                        ideologia_map[sigla] = ideologia
            else:
                print(f"Aviso: arquivo de partidos não encontrado em {ARQUIVO_PARTIDOS}")
        except Exception as e:
            print(f"Aviso: erro ao carregar ideologia dos partidos: {e}")
            ideologia_map = {}

        if ideologia_map:
            df_votos["ideologia"] = df_votos["deputado_partido"].astype(str).str.strip().str.upper().map(
                ideologia_map
            )
        else:
            df_votos["ideologia"] = None
        # Criar coluna de ano e remover colunas não desejadas
        df_votos['ano'] = df_votos['data'].dt.year
        colunas_remover = [
            'data',
            'siglaOrgao',
            'proposicaoAno',
            'proposicaoSituacao',
            'deputado_legislatura',
            'deputado_email',
            'data_registro_voto',
        ]
        df_votos = df_votos.drop(columns=[c for c in colunas_remover if c in df_votos.columns])

        # Salvar CSV de votos individuais (arquivo principal)
        df_votos.to_csv(ARQUIVO_SAIDA_VOTOS, index=False, encoding='utf-8-sig')
        print(f"\n✓ Arquivo de votos individuais salvo: {ARQUIVO_SAIDA_VOTOS}")
        print(f"  Total de linhas: {len(df_votos):,}")
        print(f"  Total de colunas: {len(df_votos.columns)}")

        # Estatísticas de votos individuais
        print("\nEstatísticas - Votos Individuais:")
        print(f"  Total de deputados únicos: {df_votos['deputado_id'].nunique()}")
        print(f"  Total de partidos únicos: {df_votos['deputado_partido'].nunique()}")
        
        # Estatísticas de fidelidade partidária
        fidelidade_stats = df_votos['fidelidade_partidaria'].value_counts(dropna=False)
        print("\nFidelidade Partidária:")
        print(f"  Seguiu orientação: {fidelidade_stats.get(True, 0):,}")
        print(f"  Não seguiu orientação: {fidelidade_stats.get(False, 0):,}")
        print(f"  Sem orientação/Liberada: {fidelidade_stats.get(None, 0):,}")
        
        # Taxa de fidelidade (apenas onde há orientação)
        fidelidade_com_orientacao = df_votos[df_votos['fidelidade_partidaria'].notna()]
        if len(fidelidade_com_orientacao) > 0:
            taxa_fidelidade = (fidelidade_com_orientacao['fidelidade_partidaria'].sum() / len(fidelidade_com_orientacao)) * 100
            print(f"  Taxa de fidelidade: {taxa_fidelidade:.2f}%")

        print("\nColunas no arquivo de votos individuais:")
        for i, col in enumerate(df_votos.columns, 1):
            print(f"  {i:2d}. {col}")
            
    except Exception as e:
        print(f"\nERRO ao criar DataFrame ou salvar arquivo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

