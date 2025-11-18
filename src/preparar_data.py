import pandas as pd

# Cargar contenido archivo resultante del merge
def cargar_data():
    macro_df = pd.read_csv("./data_clean/macro_mx_2018_2025.csv", parse_dates=["fecha"])
    return macro_df

# Convertir datos diarios a mensuales tomando el último valor de cada mes
def resampleo_data(df_diario):
    df_diario = df_diario.set_index("fecha")

    df_mes = pd.DataFrame()
    df_mes["fix"] = df_diario["fix"].resample("M").last()
    df_mes["inpc"] = df_diario["inpc"].resample("M").last()
    df_mes["tasa_obj"] = df_diario["tasa_obj"].resample("M").last()
    return df_mes

# Cálculo de variaciones
def agregar_variaciones(df_m):
    # Variaciones mensuales 
    df_m["fix_var_m"] = df_m["fix"].pct_change()*100
    df_m["inpc_var_m"] = df_m["inpc"].pct_change()*100
    df_m["tasa_obj_var_m"] = df_m["tasa_obj"].diff()

    # Variaciones anuales
    df_m["fix_var_a"] = df_m["fix"].pct_change(12)*100
    df_m["inpc_var_a"] = df_m["inpc"].pct_change(12)*100

    return df_m

if __name__=="__main__":
    df = cargar_data()
    df_month = resampleo_data(df)
    df_preparado = agregar_variaciones(df_month)

    df_preparado.to_csv("./data_clean/macro_mensual_mx_2018_2025.csv")
    print("./data_clean/macro_mensual_mx_2018_2025.csv guardado")