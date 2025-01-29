import requests
import pandas as pd
import time
import json
import sqlite3

app_id = 'ptapi679a74949e8550.60225871'

try:
    cp7_data = pd.read_csv('data/cp7_data_test.csv')
except FileNotFoundError:
    print("Erro: 'cp7_data_test.csv' não encontrado.")
    exit()

results = []

for index, row in cp7_data.iterrows():
    cp7 = str(row.get("CP7", "")).strip().replace("-", "").replace("*", "")

    if not cp7:
        results.append({'CP7': cp7, 'Concelho': 'N/A', 'Distrito': 'N/A', 'Latitude': 'N/A', 'Longitude': 'N/A', 'Morada': 'N/A'})
        continue

    url = f'https://api.duminio.com/ptcp/v2/{app_id}/{cp7}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"Erro resposta CP7 {cp7}: resposta não é um JSON válido!")
            print(f"Resposta recebida: {response.text[:500]}")
            results.append({'CP7': cp7, 'Concelho': 'N/A', 'Distrito': 'N/A', 'Latitude': 'N/A', 'Longitude': 'N/A', 'Morada': 'N/A'})
            continue

        if isinstance(data, list) and data:
            results.append({
                'CP7': cp7,
                'Concelho': data[0].get('Concelho', 'N/A'),
                'Distrito': data[0].get('Distrito', 'N/A'),
                'Latitude': data[0].get('Latitude', 'N/A'),
                'Longitude': data[0].get('Longitude', 'N/A'),
                'Morada': data[0].get('Latitude', 'N/A')
            })
        else:
            results.append({'CP7': cp7, 'Concelho': 'N/A', 'Distrito': 'N/A', 'Latitude': 'N/A', 'Longitude': 'N/A', 'Morada': 'N/A'})

    except requests.RequestException as e:
        print(f"Erro ao processar CP7 {cp7}: {e}")
        results.append({'CP7': cp7, 'Concelho': 'N/A', 'Distrito': 'N/A', 'Latitude': 'N/A', 'Longitude': 'N/A', 'Morada': 'N/A'})

    time.sleep(0.5)

results_df = pd.DataFrame(results)
results_df.to_csv('cp7_results.csv', index=False)
print('Os resultados foram salvos em cp7_results.csv')

conn = sqlite3.connect('data/cp7_data.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS cp7_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        CP7 TEXT,
        Concelho TEXT,
        Distrito TEXT,
        Latitude TEXT,
        Longitude TEXT,
        Morada TEXT
    )
''')

results_df.to_sql('cp7_results', conn, if_exists='append', index=False)
conn.commit()

print("Os resultados foram gravados no banco de dados SQLite (cp7_data.db, tabela: cp7_results).")
df = pd.read_sql("SELECT * FROM cp7_results", conn)
conn.close()

print(df)