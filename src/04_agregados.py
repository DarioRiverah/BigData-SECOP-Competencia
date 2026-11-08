"""Etapa 4 - Agregaciones con Spark para el dashboard (JSON compacto)."""
import json
from pyspark.sql import functions as F
from config import *

spark = crear_spark("SECOP-Agregados"); spark.sparkContext.setLogLevel("ERROR")
l = spark.read.parquet(LIMPIO)
cl = spark.read.parquet(str(RAIZ / "datos/procesados/asignacion_clusters.parquet"))
d = (l.join(cl, "id_proceso", "left").fillna({"cluster": -1})
       .withColumn("depto", F.coalesce(F.when(~F.col("depto_entidad").isin("No Definido"), F.col("depto_entidad")), F.lit("No definido")))
       .withColumn("mod", F.regexp_replace("modalidad", "Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes", "Selección Abreviada de Menor Cuantía"))
       .withColumn("mod", F.regexp_replace("mod", "Licitación pública Obra Publica", "Licitación pública"))
       .cache())
med = lambda: [F.count("*").alias("n"), F.sum("adj").alias("adj"), F.sum("cancelado").alias("canc"),
               F.sum("oferente_unico").alias("unico"), F.sum(F.when(F.col("adj") == 1, F.col("oferentes"))).alias("ofe"),
               F.sum(F.when(F.col("adj") == 1, F.col("valor_adj"))).alias("valor"), F.sum("visualizaciones").alias("vis"),
               F.sum("proveedor_local").alias("loc"), F.count("proveedor_local").alias("locd")]
def rows(df, cols):
    return [[r[c] if not isinstance(r[c], float) else round(r[c], 0) for c in cols] for r in df.collect()]
cubo = d.groupBy("anio", "depto", "orden_entidad", "mod", "cluster").agg(*med())
cols = ["anio", "depto", "orden_entidad", "mod", "cluster", "n", "adj", "canc", "unico", "ofe", "valor", "vis", "loc", "locd"]
seg = d.filter("segmento != ''").groupBy("segmento", "anio", "orden_entidad", "cluster").agg(*med())
cols_s = ["segmento", "anio", "orden_entidad", "cluster", "n", "adj", "canc", "unico", "ofe", "valor", "vis", "loc", "locd"]
ent = (d.filter("adj = 1").groupBy("entidad", "depto", "orden_entidad")
        .agg(F.count("*").alias("n"), F.avg("oferente_unico").alias("pu"), F.avg("oferentes").alias("po"), F.sum("valor_adj").alias("v"))
        .filter("n >= 100").orderBy(F.desc("pu")).limit(300))
hist = (d.filter("adj = 1").withColumn("b", F.least(F.col("oferentes"), F.lit(15)))
          .groupBy("cluster", "b").count())
out = {"cubo": {"cols": cols, "rows": rows(cubo, cols)}, "segmentos": {"cols": cols_s, "rows": rows(seg, cols_s)},
       "entidades": [[r.entidad, r.depto, r.orden_entidad, r.n, round(r.pu, 4), round(r.po, 2), r.v] for r in ent.collect()],
       "hist": [[r.cluster, r.b, r["count"]] for r in hist.collect()],
       "perfil_clusters": json.loads((RESULTADOS / "clusters_perfil.json").read_text()),
       "seleccion_k": json.loads((RESULTADOS / "seleccion_k.json").read_text()),
       "calidad": json.loads((RESULTADOS / "perfil_calidad.json").read_text())}
(RAIZ / "dashboard" / "datos_dashboard.json").write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":"), default=str))
print(f"Cubo de procesos   : {len(out['cubo']['rows']):>6,} filas")
print(f"Cubo de segmentos  : {len(out['segmentos']['rows']):>6,} filas")
print(f"Entidades (>=100)  : {len(out['entidades']):>6,} filas")
print("JSON exportado en dashboard/datos_dashboard.json")
spark.stop()
