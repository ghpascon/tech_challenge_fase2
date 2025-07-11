import time
import requests
import pandas as pd
from datetime import datetime
import os

def fetch_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=brl"
    save_path = ".parquet/bitcoin"

    # Cria a pasta se não existir
    os.makedirs(save_path, exist_ok=True)

    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                price = data["bitcoin"]["brl"]
                now = datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M%S")

                print(f"💰 Preço atual do Bitcoin: R$ {price}")

                # Cria DataFrame
                df = pd.DataFrame([{
                    "timestamp": now.isoformat(),
                    "price": price
                }])

                # Caminho do arquivo Parquet
                filename = f"{save_path}/btc_{timestamp}.parquet"
                df.to_parquet(filename, index=False)
                print(f"📁 Arquivo salvo em: {filename}")
                print(df)
            else:
                print(f"Erro na requisição: {response.status_code}")
        except Exception as e:
            print(f"Erro ao buscar dados: {e}")
        
        time.sleep(10)  # Aguarda 1 minuto

if __name__ == "__main__":
    fetch_btc_price()
