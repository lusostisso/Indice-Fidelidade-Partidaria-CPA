# Indice-Fidelidade-Partidaria-CPA

Análise de fidelidade partidária baseada em dados abertos da Câmara dos Deputados.

## Estrutura do Projeto

```
dados_coletados/
├── dados_detalhes/
│   ├── orientacoes/        # Orientações de voto por partido/bloco
│   └── votos/              # Votos individuais de deputados
├── dados_partidos/
│   └── partidos_existentes.csv
└── dados_votacoes/
    ├── votacoes_*.json     # Metadados das votações
    └── proposicaoTema_*.json  # Temas das proposições
scripts_coleta_de_dados/
├── coletar_detalhes_votacoes.py
├── coletar_temas_proposicoes.py
├── detalhes_votacoes.py
├── votacoes.py
└── gerar_dataset_powerbi.py  # Script de geração do dataset limpo
```

## Geração do Dataset para Power BI

O script `gerar_dataset_powerbi.py` processa os dados coletados e gera um arquivo consolidado pronto para importação no Power BI.

### Regras de Validação

O dataset gerado inclui apenas:
- **Votações com votos registrados**: descarta votações sem votos
- **Votações com orientações de partido**: descarta votações sem orientação explícita de partido (codTipoLideranca='P')
- **Votos de deputados cujo partido possui orientação**: cada linha representa um voto individual alinhado à orientação do partido

### Uso

```bash
# Navegue até o diretório dos scripts
cd scripts_coleta_de_dados

# Gerar dataset com todos os anos disponíveis (padrão):
python gerar_dataset_powerbi.py

# Especificar arquivo de saída:
python gerar_dataset_powerbi.py --saida meu_dataset.csv

# Processar apenas anos específicos:
python gerar_dataset_powerbi.py --anos 2018,2019

# Gerar em formato parquet (requer pandas e pyarrow):
python gerar_dataset_powerbi.py --parquet --saida dataset_powerbi.parquet
```

### Colunas do Dataset

| Coluna | Descrição |
|--------|-----------|
| `ano` | Ano da votação |
| `idVotacao` | Identificador único da votação |
| `dataVotacao` | Data da votação |
| `siglaOrgao` | Sigla do órgão (ex: PLEN) |
| `descricao` | Descrição da votação |
| `aprovacao` | 1=aprovado, 0=rejeitado |
| `deputadoId` | ID do(a) deputado(a) |
| `deputadoNome` | Nome do(a) deputado(a) |
| `deputadoPartido` | Partido do(a) deputado(a) |
| `deputadoUf` | UF do(a) deputado(a) |
| `tipoVoto` | Voto registrado (Sim/Não/Abstenção/Obstrução) |
| `orientacaoPartido` | Orientação do partido para a votação |
| `seguiuOrientacao` | 1=seguiu, 0=não seguiu |
| `proposicaoId` | ID da proposição votada |
| `proposicaoSiglaTipo` | Tipo da proposição (PL, PEC, etc.) |
| `proposicaoNumero` | Número da proposição |
| `proposicaoAno` | Ano da proposição |
| `proposicaoSituacao` | Situação atual da proposição |
| `temas` | Temas da proposição (separados por \|) |

### Exemplo de Resultado

Após execução bem-sucedida:
```
[INFO] Anos detectados automaticamente: 2018, 2019, 2020, 2021, 2022
Votações válidas: 2316
Votos escritos: 739183
Arquivo gerado: dataset_powerbi.csv
```

## Importação no Power BI

1. Abra o Power BI Desktop
2. Clique em **Obter Dados** > **Texto/CSV**
3. Selecione o arquivo `dataset_powerbi.csv` gerado
4. Configure os tipos de dados conforme necessário
5. Crie medidas e visualizações para análise de fidelidade partidária

## Requisitos

- Python 3.7+
- Bibliotecas opcionais:
  - `pandas` e `pyarrow` (apenas para formato parquet)
