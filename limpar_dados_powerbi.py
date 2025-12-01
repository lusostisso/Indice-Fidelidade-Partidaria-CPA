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
from pathlib import Path
from typing import Dict, List, Optional, Set
import pandas as pd
from datetime import datetime

# Configurações
PASTA_DADOS = "dados_coletados"
ANOS = [2018, 2019, 2020, 2021, 2022]
ARQUIVO_SAIDA = "dados_limpos_powerbi.csv"


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


def carregar_temas_proposicoes(ano: int) -> Dict[str, List[dict]]:
    """Carrega os temas das proposições de um ano."""
    arquivo = os.path.join(PASTA_DADOS, "dados_votacoes", f"proposicaoTema_{ano}.json")
    dados = carregar_json(arquivo)
    if not dados:
        return {}
    
    # Criar dicionário indexado por ID da proposição
    temas_dict = {}
    for item in dados:
        prop_id = str(item.get("id"))
        temas = item.get("temas", [])
        if temas:
            temas_dict[prop_id] = temas
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


def extrair_temas_da_votacao(votacao_detalhes: dict, temas_proposicoes: Dict[str, List[dict]]) -> List[str]:
    """Extrai os temas das proposições relacionadas à votação."""
    temas_encontrados = []
    
    # Verificar proposicoesAfetadas
    proposicoes_afetadas = votacao_detalhes.get("proposicoesAfetadas", [])
    for prop in proposicoes_afetadas:
        prop_id = str(prop.get("id"))
        if prop_id in temas_proposicoes:
            for tema in temas_proposicoes[prop_id]:
                tema_nome = tema.get("tema")
                if tema_nome and tema_nome not in temas_encontrados:
                    temas_encontrados.append(tema_nome)
    
    return temas_encontrados


def validar_votacao(votacao_id: str, votos: List[dict], orientacoes: List[dict]) -> bool:
    """
    Valida se uma votação tem pelo menos votos OU orientações.
    
    Uma votação só é descartada se NÃO tiver votos E NÃO tiver orientações.
    Se tiver pelo menos um dos dois, é válida.
    """
    tem_votos = votos and len(votos) > 0
    tem_orientacoes = orientacoes and len(orientacoes) > 0
    
    # Verificar se há orientações válidas (não nulas)
    if tem_orientacoes:
        orientacoes_validas = [o for o in orientacoes if o.get("orientacaoVoto") is not None]
        tem_orientacoes = len(orientacoes_validas) > 0
    
    # Votação é válida se tiver votos OU orientações
    return tem_votos or tem_orientacoes


def processar_votacao(votacao_id: str, 
                     votacao_basica: dict,
                     votacao_detalhes: dict,
                     votos: List[dict],
                     orientacoes: List[dict],
                     temas_proposicoes: Dict[str, List[dict]]) -> Optional[dict]:
    """Processa uma votação e retorna um dicionário com os dados limpos."""
    
    # Validar votação
    if not validar_votacao(votacao_id, votos, orientacoes):
        return None
    
    # Extrair temas
    temas = extrair_temas_da_votacao(votacao_detalhes, temas_proposicoes)
    
    # Extrair informações básicas
    dados_limpos = {
        "id_votacao": votacao_id,
        "data": votacao_basica.get("data"),
        "dataHoraRegistro": votacao_basica.get("dataHoraRegistro"),
        "siglaOrgao": votacao_basica.get("siglaOrgao"),
        "descricao": votacao_basica.get("descricao"),
        "aprovacao": votacao_basica.get("aprovacao"),
        "proposicaoObjeto": votacao_basica.get("proposicaoObjeto"),
        
        # Informações dos detalhes
        "idOrgao": votacao_detalhes.get("idOrgao"),
        "idEvento": votacao_detalhes.get("idEvento"),
        "descUltimaAberturaVotacao": votacao_detalhes.get("descUltimaAberturaVotacao"),
        "dataHoraUltimaAberturaVotacao": votacao_detalhes.get("dataHoraUltimaAberturaVotacao"),
        
        # Proposições afetadas (primeira, se houver)
        "proposicao_afetada_id": None,
        "proposicao_afetada_siglaTipo": None,
        "proposicao_afetada_numero": None,
        "proposicao_afetada_ano": None,
        "proposicao_afetada_ementa": None,
        
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
    
    # Extrair primeira proposição afetada (se houver)
    proposicoes_afetadas = votacao_detalhes.get("proposicoesAfetadas", [])
    if proposicoes_afetadas:
        primeira_prop = proposicoes_afetadas[0]
        dados_limpos["proposicao_afetada_id"] = primeira_prop.get("id")
        dados_limpos["proposicao_afetada_siglaTipo"] = primeira_prop.get("siglaTipo")
        dados_limpos["proposicao_afetada_numero"] = primeira_prop.get("numero")
        dados_limpos["proposicao_afetada_ano"] = primeira_prop.get("ano")
        dados_limpos["proposicao_afetada_ementa"] = primeira_prop.get("ementa")
    
    return dados_limpos


def processar_ano(ano: int) -> List[dict]:
    """Processa todos os dados de um ano."""
    print(f"\nProcessando ano {ano}...")
    
    # Carregar todos os dados
    votacoes_basicas = carregar_votacoes(ano)
    votacoes_detalhes = carregar_votacoes_detalhes(ano)
    temas_proposicoes = carregar_temas_proposicoes(ano)
    votos = carregar_votos(ano)
    orientacoes = carregar_orientacoes(ano)
    
    print(f"  Votações básicas: {len(votacoes_basicas)}")
    print(f"  Votações detalhes: {len(votacoes_detalhes)}")
    print(f"  Temas proposições: {len(temas_proposicoes)}")
    print(f"  Votos: {len(votos)}")
    print(f"  Orientações: {len(orientacoes)}")
    
    # Processar cada votação
    dados_limpos = []
    votacoes_validas = 0
    votacoes_invalidas = 0
    votacoes_sem_dados = 0
    
    # Usar IDs que estão em votacoes_basicas como base
    # Criar um conjunto de IDs únicos (normalizados) para evitar processar duplicados
    ids_processados = set()
    
    for votacao_id in votacoes_basicas.keys():
        # Normalizar ID para evitar processar duplicados
        id_normalizado = normalizar_id_votacao(votacao_id)
        if id_normalizado in ids_processados:
            continue
        ids_processados.add(id_normalizado)
        
        votacao_basica = votacoes_basicas.get(votacao_id)
        if not votacao_basica:
            # Tentar buscar pelo ID equivalente
            votacao_basica = buscar_id_equivalente(votacao_id, votacoes_basicas)
            if not votacao_basica:
                votacoes_sem_dados += 1
                continue
        
        # Buscar detalhes, votos e orientações usando busca equivalente
        # Tentar primeiro pelo ID exato, depois pelo ID base
        votacao_detalhe = buscar_id_equivalente(votacao_id, votacoes_detalhes) or {}
        votos_votacao = buscar_id_equivalente(votacao_id, votos) or []
        orientacoes_votacao = buscar_id_equivalente(votacao_id, orientacoes) or []
        
        # Usar o ID original da votação básica se encontrado
        votacao_id_final = votacao_basica.get("id", votacao_id)
        
        # Processar votação
        dados = processar_votacao(
            votacao_id_final,
            votacao_basica,
            votacao_detalhe,
            votos_votacao,
            orientacoes_votacao,
            temas_proposicoes
        )
        
        if dados:
            dados_limpos.append(dados)
            votacoes_validas += 1
        else:
            votacoes_invalidas += 1
    
    print(f"  Votações válidas: {votacoes_validas}")
    print(f"  Votações inválidas (sem orientações e sem votos): {votacoes_invalidas}")
    if votacoes_sem_dados > 0:
        print(f"  Votações sem dados básicos: {votacoes_sem_dados}")
    
    return dados_limpos


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
    todos_dados = []
    for ano in ANOS:
        try:
            dados_ano = processar_ano(ano)
            todos_dados.extend(dados_ano)
        except Exception as e:
            print(f"ERRO ao processar ano {ano}: {e}")
            continue
    
    if len(todos_dados) == 0:
        print("\nERRO: Nenhum dado válido foi encontrado!")
        return
    
    print(f"\n{'=' * 60}")
    print(f"Total de votações válidas: {len(todos_dados)}")
    print(f"{'=' * 60}")
    
    # Criar DataFrame
    try:
        df = pd.DataFrame(todos_dados)
        
        # Ordenar por data
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
        df = df.sort_values('data')
        
        # Salvar CSV
        df.to_csv(ARQUIVO_SAIDA, index=False, encoding='utf-8-sig')
        print(f"\n✓ Arquivo salvo: {ARQUIVO_SAIDA}")
        print(f"  Total de linhas: {len(df)}")
        print(f"  Total de colunas: {len(df.columns)}")
        
        # Mostrar estatísticas
        print("\nEstatísticas:")
        print(f"  Período: {df['data'].min()} a {df['data'].max()}")
        print(f"  Total de votos registrados: {df['total_votos'].sum():,}")
        print(f"  Total de orientações registradas: {df['total_orientacoes'].sum():,}")
        print(f"  Votações com temas: {df['temas'].notna().sum()}")
        
        # Estatísticas por ano
        df['ano'] = df['data'].dt.year
        print("\nVotações por ano:")
        for ano in sorted(df['ano'].dropna().unique()):
            count = len(df[df['ano'] == ano])
            print(f"  {int(ano)}: {count:,} votações")
        
        print("\nColunas no arquivo:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
            
    except Exception as e:
        print(f"\nERRO ao criar DataFrame ou salvar arquivo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

