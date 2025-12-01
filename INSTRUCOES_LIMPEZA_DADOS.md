# Instruções para Limpeza de Dados

## Descrição

Este script (`limpar_dados_powerbi.py`) consolida e limpa os dados coletados da API da Câmara dos Deputados para gerar um arquivo CSV pronto para uso no Power BI.

## Requisitos

- Python 3.7 ou superior
- Biblioteca pandas: `pip install pandas`

## Estrutura de Dados Esperada

O script espera a seguinte estrutura de pastas:

```
dados_coletados/
├── dados_votacoes/
│   ├── votacoes_2018.json
│   ├── votacoes_2019.json
│   ├── votacoes_2020.json
│   ├── votacoes_2021.json
│   ├── votacoes_2022.json
│   ├── votacoesID_2018.json
│   ├── votacoesID_2019.json
│   ├── votacoesID_2020.json
│   ├── votacoesID_2021.json
│   ├── votacoesID_2022.json
│   ├── proposicaoTema_2018.json
│   ├── proposicaoTema_2019.json
│   ├── proposicaoTema_2020.json
│   ├── proposicaoTema_2021.json
│   └── proposicaoTema_2022.json
└── dados_detalhes/
    ├── votos/
    │   ├── 2018.json
    │   ├── 2019.json
    │   ├── 2020.json
    │   ├── 2021.json
    │   └── 2022.json
    └── orientacoes/
        ├── 2018.json
        ├── 2019.json
        ├── 2020.json
        ├── 2021.json
        └── 2022.json
```

## Como Usar

1. Certifique-se de que todos os arquivos de dados estão na pasta `dados_coletados` conforme a estrutura acima.

2. Instale a dependência necessária:
   ```bash
   pip install pandas
   ```

3. Execute o script:
   ```bash
   python limpar_dados_powerbi.py
   ```

4. O script irá:
   - Carregar todos os arquivos de dados por ano
   - Juntar as informações usando o ID de votação como chave
   - Filtrar votações sem orientações ou sem votos
   - Gerar dois arquivos CSV:
     - `dados_limpos_powerbi.csv`: Tabela agregada de votações
     - `votos_deputados_powerbi.csv`: Tabela detalhada de votos individuais dos deputados

## Critérios de Validação

Uma votação será incluída no arquivo final apenas se:

1. ✅ Possuir votos de deputados (não vazio)
2. ✅ Possuir orientações de partidos (não vazio)
3. ✅ Possuir pelo menos uma orientação válida (não nula)

Votações que não atendem a esses critérios são descartadas.

## Estrutura dos Arquivos de Saída

### Arquivo 1: `dados_limpos_powerbi.csv` (Votações Agregadas)

Este arquivo contém uma linha por votação com informações agregadas:

### Informações Básicas da Votação
- `id_votacao`: ID único da votação
- `data`: Data da votação
- `dataHoraRegistro`: Data e hora do registro
- `siglaOrgao`: Sigla do órgão
- `descricao`: Descrição da votação
- `aprovacao`: Se foi aprovada (1) ou não (0)
- `proposicaoObjeto`: Proposição objeto da votação

### Informações Detalhadas
- `idOrgao`: ID do órgão
- `idEvento`: ID do evento
- `descUltimaAberturaVotacao`: Descrição da última abertura
- `dataHoraUltimaAberturaVotacao`: Data/hora da última abertura

### Informações da Proposição
- `proposicao_afetada_id`: ID da proposição afetada
- `proposicao_afetada_siglaTipo`: Tipo da proposição (PL, MPV, etc.)
- `proposicao_afetada_numero`: Número da proposição
- `proposicao_afetada_ano`: Ano da proposição
- `proposicao_afetada_ementa`: Ementa da proposição

### Temas
- `temas`: Temas relacionados (separados por "; ")
- `quantidade_temas`: Quantidade de temas

### Estatísticas de Votos
- `total_votos`: Total de votos registrados
- `votos_sim`: Quantidade de votos "Sim"
- `votos_nao`: Quantidade de votos "Não"
- `votos_abstencao`: Quantidade de abstenções
- `votos_obstrucao`: Quantidade de obstruções
- `votos_outros`: Outros tipos de voto

### Estatísticas de Orientações
- `total_orientacoes`: Total de orientações de partidos
- `orientacoes_sim`: Quantidade de orientações "Sim"
- `orientacoes_nao`: Quantidade de orientações "Não"
- `orientacoes_abstencao`: Quantidade de orientações "Abstenção"
- `orientacoes_liberada`: Quantidade de orientações "Liberada"
- `orientacoes_outras`: Outras orientações

### Arquivo 2: `votos_deputados_powerbi.csv` (Votos Individuais)

Este arquivo contém uma linha para cada voto de cada deputado em cada votação. Permite análises detalhadas de fidelidade partidária.

#### Informações da Votação (para fazer joins)
- `id_votacao`: ID único da votação
- `data`: Data da votação
- `siglaOrgao`: Sigla do órgão
- `descricao`: Descrição da votação
- `aprovacao`: Se foi aprovada (1) ou não (0)
- `temas`: Temas relacionados (separados por "; ")
- `proposicao_afetada_id`: ID da proposição afetada
- `proposicao_afetada_siglaTipo`: Tipo da proposição
- `proposicao_afetada_numero`: Número da proposição
- `proposicao_afetada_ano`: Ano da proposição

#### Informações do Deputado
- `deputado_id`: ID único do deputado
- `deputado_nome`: Nome do deputado
- `deputado_partido`: Sigla do partido
- `deputado_uf`: Sigla do estado (UF)
- `deputado_legislatura`: ID da legislatura
- `deputado_email`: Email do deputado (se disponível)

#### Informações do Voto
- `voto`: Tipo de voto do deputado ("Sim", "Não", "Abstenção", "Obstrução", etc.)
- `data_registro_voto`: Data e hora do registro do voto

#### Informações de Fidelidade Partidária
- `orientacao_partido`: Orientação de voto do partido do deputado ("Sim", "Não", "Abstenção", "Liberada", ou NULL)
- `fidelidade_partidaria`: 
  - `True`: Deputado seguiu a orientação do partido
  - `False`: Deputado não seguiu a orientação do partido
  - `NULL`: Não há orientação do partido ou orientação é "Liberada"

## Importação no Power BI

1. Abra o Power BI Desktop
2. Clique em "Obter Dados" > "Texto/CSV"
3. Importe ambos os arquivos:
   - `dados_limpos_powerbi.csv` (tabela de votações)
   - `votos_deputados_powerbi.csv` (tabela de votos individuais)
4. Configure a codificação como UTF-8 se necessário
5. Crie um relacionamento entre as tabelas usando `id_votacao` como chave
6. Clique em "Carregar"

### Relacionamento entre Tabelas

No Power BI, crie um relacionamento:
- **Tabela 1**: `dados_limpos_powerbi` (um para muitos)
- **Tabela 2**: `votos_deputados_powerbi` (muitos)
- **Chave**: `id_votacao`

## Análises Possíveis

Com os dados gerados, você pode realizar análises como:

1. **Fidelidade Partidária por Deputado**: Calcular a porcentagem de vezes que cada deputado seguiu a orientação do partido
2. **Fidelidade por Tema**: Verificar se a fidelidade varia conforme o tema da votação
3. **Fidelidade ao Longo do Tempo**: Analisar se há mudanças na fidelidade conforme o período eleitoral se aproxima
4. **Comportamento dos Partidos**: Comparar quantas orientações cada partido emite e como os deputados respondem
5. **Análise por Região**: Verificar se há padrões regionais na fidelidade partidária

## Notas

- Os arquivos são salvos com codificação UTF-8-sig para compatibilidade com Excel/Power BI
- As datas são convertidas para formato datetime
- Os arquivos são ordenados por data (mais antigas primeiro)
- Votações sem temas ainda são incluídas (campo `temas` será NULL)
- A fidelidade partidária só é calculada quando há uma orientação clara do partido (não "Liberada" ou NULL)
- IDs de votação são normalizados para fazer match entre formatos com e sem sufixo (ex: "2152544-73" e "2152544")

