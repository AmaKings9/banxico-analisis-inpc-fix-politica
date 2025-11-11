from dotenv import load_dotenv

import requests
import pandas as pd
import os
import json
from datetime import datetime

# Cargar variables de entorno (token)
load_dotenv()
TOKEN = os.getenv("BX_TOKEN")

def extraer_datos(series_id, start_date=None, end_date=None, token=TOKEN):
    '''
    ### Por el momento la api de banxico redirige de forma automática a 
        https://anterior.banxico.org.mx/SieAPIRest/service/v1/series/{}/datos/2018-01-01/
        por lo que no se puede hacer uso de la api y se realizo la extraccion manualmente

    url_default = "https://api.banxico.org.mx/SieAPIRest/service/v1/series/{}/datos"
    url_default = url_default.format(series_id)

    # Filtros de consulta (fechas)

    if start_date:
        url_default = url_default + f"/{start_date}"
    if end_date:
        url_default = url_default + f"/{end_date}"
    
    # Autenticacion credenciales
    headers = {"Bmx-Token": token}
    
    print(url_default)
    r = requests.get(url_default, headers=headers)
    r.raise_for_status()
    print(r)
    '''

    with open(series_id) as f:
        data = json.load(f)

    datos = data['bmx']['series'][0]['datos']
    df = pd.DataFrame(datos)
    df = df.rename(columns={'fecha':'fecha','dato':'valor'})
    df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
    df['valor'] = pd.to_numeric(df['valor'].str.replace(',',''), errors='coerce')
    return df[['fecha','valor']]

if __name__ == "__main__":
    series = {
        "inpc": "data_raw/inpc_muestra_2.json",
        "fix": "data_raw/fix_muestra.json",
        "tasa_obj": "data_raw/tasa_objetivo_muestra.json"
    }
    
    for name, serie in series.items():
        df_s = extraer_datos(serie, start_date="2018-01-01")
        df_s.to_csv(f"data_raw/{name}.csv", index=False)
        print(f"data_raw/{name}.csv guardado")