# Competencia en la contratación pública colombiana – SECOP II

Actividad del curso **Big Data** · Especialización en Ciencia de Datos · Universidad Santo Tomás, Seccional Tunja
Darío Sebastián Rivera Sáenz

¿Hay competencia real en los procesos de contratación que, por ley, deberían ser competitivos? En este repositorio está el código con el que procesé los **9,3 millones de registros (10,3 GB)** de SECOP II con PySpark, el modelo de **clustering K-Means** (Spark MLlib) y el **dashboard en Power BI**.

**Dashboard publicado:** [Power BI](https://app.powerbi.com/view?r=eyJrIjoiNmQ3YTU4MDctYjU4Yy00ZDkyLTljOTItNzRkYzk5OTBiNmIyIiwidCI6Ijk5YzY3NGNhLWMwYzctNDkwYS1hZThmLTE2MTcyNjM2ZjMxYiIsImMiOjR9)

## Datos

- Fuente: Colombia Compra Eficiente, *SECOP II – Procesos de Contratación*, [datos.gov.co (p6dx-8zbt)](https://www.datos.gov.co/d/p6dx-8zbt).
- El CSV original no se sube al repositorio por su tamaño. Hay que descargarlo y dejarlo una carpeta arriba del proyecto con el nombre `SecopII_ProcesosDeContratacion.csv`.
- A diferencia de lo visto en clase, uso campos de **competencia**: proveedores únicos con respuesta, visualizaciones, proveedores invitados, duración, valor adjudicado, código UNSPSC y departamento del proveedor.

## Estructura

```
src/
  config.py           rutas, modalidades competitivas y sesión de Spark
  01_ingesta.py       CSV de 10 GB -> 24 bloques Parquet (lectura por bloques, se puede reanudar)
  02_limpieza.py      perfil de calidad, filtro de modalidades competitivas, tipificación e indicadores
  03_clustering.py    selección de k (silueta y WSSSE) y K-Means final con k = 4
  04_agregados.py     tablas agregadas para el dashboard
powerbi/              proyecto de Power BI (.pbip): modelo semántico en TMDL y definición del informe
```

## Cómo ejecutarlo (Ubuntu / WSL)

```bash
sudo apt install -y openjdk-17-jdk python3-venv
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cd src
python 01_ingesta.py          # si se interrumpe, se vuelve a ejecutar y continúa
python 02_limpieza.py
python 03_clustering.py       # prueba k = 2 a 8
python 03_clustering.py 4     # modelo final
python 04_agregados.py
```

Los scripts dejan sus salidas en `resultados/` y los datos procesados en `datos/procesados/` (no se versionan). Para abrir el dashboard se abre `powerbi/Dashboard_SECOP_Competencia.pbip` en Power BI Desktop; las tablas que usa el modelo se generan a partir de las agregaciones de Spark y su ruta se ajusta en *Transformar datos*.

## Resultados principales

| Indicador | Valor |
|---|---|
| Procesos competitivos analizados (2018–2026) | 734.815 |
| Procesos adjudicados | 412.816 |
| Adjudicados con un solo oferente | 39,4 % |
| Oferentes promedio por proceso | 4,0 |

| Perfil (K-Means, k = 4) | % procesos | % valor | % oferente único |
|---|---|---|---|
| Competencia robusta | 26,1 % | 74,3 % | 9,2 % |
| Baja visibilidad | 45,7 % | 5,5 % | 53,4 % |
| Compra exprés | 18,4 % | 6,1 % | 48,6 % |
| Invitación dirigida | 9,8 % | 14,1 % | 37,0 % |

> Los indicadores son alertas para revisar, no pruebas de irregularidades.
