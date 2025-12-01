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
   - Gerar o arquivo `dados_limpos_powerbi.csv`

## Critérios de Validação

Uma votação será incluída no arquivo final apenas se:

1. ✅ Possuir votos de deputados (não vazio)
2. ✅ Possuir orientações de partidos (não vazio)
3. ✅ Possuir pelo menos uma orientação válida (não nula)

Votações que não atendem a esses critérios são descartadas.

## Estrutura do Arquivo de Saída

O arquivo `dados_limpos_powerbi.csv` contém as seguintes colunas:

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

## Importação no Power BI

1. Abra o Power BI Desktop
2. Clique em "Obter Dados" > "Texto/CSV"
3. Selecione o arquivo `dados_limpos_powerbi.csv`
4. Configure a codificação como UTF-8 se necessário
5. Clique em "Carregar"

## Notas

- O arquivo é salvo com codificação UTF-8-sig para compatibilidade com Excel/Power BI
- As datas são convertidas para formato datetime
- O arquivo é ordenado por data (mais antigas primeiro)
- Votações sem temas ainda são incluídas (campo `temas` será NULL)

