import pandas as pd

# Cargar contenido archivos csv
def cargar_data():
    inpc_df = pd.read_csv("./data_raw/inpc.csv")
    fix_df = pd.read_csv("./data_raw/fix.csv")
    tasa_obj_df = pd.read_csv("./data_raw/tasa_obj.csv")
    return inpc_df, fix_df, tasa_obj_df

# Convertir la columna fecha a datetime
# Ordenar valores por fecha
# Cambiar nombre de las columnas de valor
def formato_fecha(df, nombre_df):
    df["fecha"] = pd.to_datetime(df["fecha"], format='%Y-%m-%d')
    df = df.sort_values("fecha").reset_index(drop=True)
    df = df.rename(columns={"valor": nombre_df})
    return df

# Merge out para combinar todos los datos de los 3 datasets
# Forward fill a tasas objetivo para preservar continuidad 
def combinar_data(df_inpc, df_fix, df_tasa_obj):
    df_final = (df_fix.merge(df_inpc, how="outer", on="fecha")).merge(df_tasa_obj, how="outer", on="fecha")
    df_final = df_final.sort_values(by="fecha").reset_index(drop=True)
    df_final["tasa_obj"] = df_final["tasa_obj"].ffill()
    return df_final

# Guardar dataset limpio
def guardar_data(df_limpio):
    df_limpio.to_csv("./data_clean/macro_mx_2018_2025.csv")
    print("macro_mx_2018_2025.csv, guardado")

if __name__ == "__main__":
    print("Cargando datos...")
    df_inpc, df_fix, df_tasa_obj = cargar_data()

    print("Cambiando formato de fecha en datasets...")
    df_inpc = formato_fecha(df_inpc, "inpc")
    df_fix = formato_fecha(df_fix, "fix")
    df_tasa_obj = formato_fecha(df_tasa_obj, "tasa_obj")

    print("Uniendo datasets en uno solo...")
    df_union = combinar_data(df_inpc, df_fix, df_tasa_obj)

    print("Guardando dataset limpio y unido...")
    guardar_data(df_union)