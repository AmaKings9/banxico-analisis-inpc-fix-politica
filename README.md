# banxico-analisis-inpc-fix-politica
# Análisis de Inflación, Tasa Objetivo y Tipo de Cambio (2018--2025)

Este proyecto realiza un análisis exploratorio para estudiar la evolución y relación entre:

-   **Inflación (variación anual del INPC)**
-   **Tasa objetivo (Banxico)**
-   **Tipo de cambio FIX**

## 📝 Descripción detallada del proyecto

Para leer una explicación detallada del proceso de desarrollo del proyecto puede revisar el
archivo `documentacion.ipynb`, el cual incluye:

- Delimitación del objetivo del proyecto y preguntas a responder 
- Descripción de cada fase del proceso de desarrollo
- Descripción e interpretación de los gráficos obtenidos

## 📌 Objetivos del proyecto

1.  Analizar la evolución temporal de las variables macroeconómicas.
2.  Obtener variaciones mensuales y anuales necesarias para el análisis.
3.  Contestar preguntas de política monetaria:
    -   ¿Cómo han evolucionado las variables?
    -   ¿La tasa objetivo responde a cambios en la inflación?
    -   ¿Los cambios en la tasa objetivo explican movimientos
        del tipo de cambio?

## 📂 Estructura del proyecto

    ├── data_clean/
    │   ├── macro_mensual_mx_2018_2025.csv
    │   ├── macro_mx_2018_2025.csv
    ├── data_raw/
    │   ├── fix_muestra.json
    │   ├── inpc_muestra.json
    │   ├── tasa_objetivo_muestra.json
    │   ├── fix.csv
    │   ├── inpc.csv
    │   ├── tasa_obj.csv
    ├── figures/
    ├── notebooks/
    │   ├── documentacion.ipynb
    │   ├── exploracion_data.ipynb
    ├── src/
    │   ├── extraer_data.py
    │   ├── limpieza_data.py
    │   ├── preparar_data.py
    ├── README.md

## 🔍 Extracción de los datos

El archivo `extraer_data.py` inlcuye:

- Extracción de los datos a través de la API oficial de Banxico
- Almacenamiento cada índice económico y sus atributos en formato json

## 🧹 Limpieza de los datos

El archivo `limpieza_data.py` incluye:

- Unión de los datasets de los índices económicos en un solo archivo .csv
- Reemplazo de valores nulos

## 🔧 Preparación de los datos

El archivo `preparar_data.py` incluye:

-   Resampleo a frecuencia mensual
-   Cálculo de:
    -   Inflación mensual
    -   Inflación anual
    -   Variación mensual del tipo de cambio
    -   Cambios mensuales y anuales de la tasa objetivo

## 📈 Gráficas incluidas

El cuaderno de Jupyter `exploracion_data.ipynb` genera (como archivo PNG):

### 1. **Evolución temporal de inflación, tasa objetivo y tipo de cambio**

Responde: - *¿Cómo han evolucionado las variables entre 2018 y 2025?*
Responde: - *¿Hay periodos donde la variable cambia de forma destacada?*

### 2. **Inflación anual vs. tasa objetivo (líneas)**

Responde: - *¿La tasa objetivo responde a cambios en la inflación?*

### 3. **Tipo de cambio vs. tasa objetivo**

Responde: - *¿Los cambios en la tasa objetivo explican los movimientos 
del tipo de cambio?*

## ▶️ Cómo ejecutar

1.  Instalar dependencias:

```{=html}
<!-- -->
```
    pip install pandas matplotlib requests 

2.  Ejecutar preparación:

```{=html}
<!-- -->
```
    python src/extraer_data.py
    python src/limpieza_data.py
    python src/preparar_data.py

3.  Generar gráficas:

```{=html}
<!-- -->
```
    python src/exploracion_data.ipynb

## 📘 Notas finales

Este proyecto es una introducción práctica al análisis macroeconómico
aplicado con Python, orientado a desarrollar criterios para interpretar
la política monetaria y su impacto en precios y tipo de cambio.

