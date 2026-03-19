## Pipeline ETL em Python: Cotações de Criptomoedas
Este é um projeto de Engenharia de Dados focado na construção de um pipeline **ETL (Extract, Transform, Load)** automatizado. O objetivo do script é consumir dados em tempo real do mercado financeiro de criptomoedas, tratá-los e armazená-los em um banco de dados relacional para futuras análises.

## Arquitetura do Pipeline
O fluxo de dados segue três etapas:
1. **Extract (Extração):** Consumo da API pública da [CoinGecko](https://www.coingecko.com/en/api) via requisição HTTP (`requests`), buscando as 10 principais criptomoedas por valor de mercado.
2. **Transform (Transformação):** Processamento do arquivo JSON bruto utilizando a biblioteca `pandas`. Os dados são convertidos em um DataFrame, filtrados apenas para as colunas essenciais.
3. **Load (Carga):** Ingestão dos dados limpos em um banco de dados relacional **SQLite** local (`sqlite3`), estruturando-os em uma tabela pronta para consultas SQL.

## Tecnologias Utilizadas
* **Linguagem:** Python 3
* **Bibliotecas:** `pandas`, `requests`, `sqlite3`
* **Banco de Dados:** SQLite
* **Fonte de Dados:** CoinGecko API

## Como Executar o Projeto
1. Clone este repositório:
git clone [https://github.com/seu-usuario/projeto_etl_api.git](https://github.com/seu-usuario/projeto_etl_api.git)

2. Acesse a pasta do projeto:
"cd projeto_etl_api"

3. Crie e ative um ambiente virtual:
"python -m venv venv"
# No Windows:
"venv\Scripts\activate"

4. Instale as dependências:
"pip install -r requirements.txt"

5. Execute o pipeline
"python main.py"

