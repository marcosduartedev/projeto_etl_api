import requests
import pandas as pd
import sqlite3

def extrair_dados_crypto():
    url = "https://api.coingecko.com/api/v3/coins/markets"

    parametros = {
        "vs_currency": "brl",
        "order": "market_cap_desc",
        "per_page": 5,
        "page": 1,
        "sparkline": "false"
    }

    print("Extraindo dados da API...")
    resposta = requests.get(url, params=parametros)

    if resposta.status_code == 200:
        dados = resposta.json()
        print("Dados extraídos com sucesso!")
        return dados
    else:
        print("Erro ao extrair dados: {resposta.status_code}")
        return None


def transformar_dados(dados_brutos):
    print("Transformando dados...")
    df = pd.DataFrame(dados_brutos)
    colunas_desejadas = ['name', 'symbol', 'current_price', 'market_cap', 'total_volume']
    df_limpo = df[colunas_desejadas]
    df_limpo = df_limpo.rename(columns={
        'name': 'Nome',
        'symbol': 'Símbolo',
        'current_price': 'Preço (BRL)',
        'market_cap': 'Valor de Mercado',
        'total_volume': 'Volume (24h)'
    })

    print("Dados transformados com sucesso!")
    return df_limpo


def carregar_dados(df_limpo):
    print("iniciando a carga no Banco de Dados...")
    conexao = sqlite3.connect('criptomoedas.db')
    df_limpo.to_sql('cotacoes_cripto', conexao, if_exists='replace', index=False)
    conexao.close()
    print("Dados salvos com sucesso no banco 'crypto_dados.db'!")



if __name__ == "__main__":
    dados_brutos = extrair_dados_crypto()

    if dados_brutos:
        dados_transformados = transformar_dados(dados_brutos)
        print("\n---")
        carregar_dados(dados_transformados)
        print("\nPipeline ETL finalizado com sucesso!")