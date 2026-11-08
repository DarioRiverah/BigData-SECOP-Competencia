"""Etapa 3 - Clustering K-Means (Spark MLlib): perfiles de competencia de los
procesos competitivos adjudicados."""
import json, sys
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from config import *

K_FINAL = int(sys.argv[1]) if len(sys.argv) > 1 else 0
spark = crear_spark("SECOP-KMeans"); spark.sparkContext.setLogLevel("ERROR")
df = (spark.read.parquet(LIMPIO).filter("adj = 1")
      .dropna(subset=["oferentes", "visualizaciones", "invitados", "duracion_dias", "valor_adj"])
      .withColumn("f_oferentes", F.log1p("oferentes"))
      .withColumn("f_visualizaciones", F.log1p("visualizaciones"))
      .withColumn("f_invitados", F.log1p("invitados"))
      .withColumn("f_duracion", F.log1p("duracion_dias"))
      .withColumn("f_valor", F.log10("valor_adj")))
FEATS = ["f_oferentes", "f_visualizaciones", "f_invitados", "f_duracion", "f_valor"]
prep = Pipeline(stages=[VectorAssembler(inputCols=FEATS, outputCol="raw"),
                        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)])
ev = ClusteringEvaluator(featuresCol="features", metricName="silhouette")

if K_FINAL == 0:   # selección de k con silhouette y codo (WSSSE) sobre muestra
    m = df.sample(False, 0.25, seed=42).cache()
    print(f"Muestra para selección de k: {m.count():,} procesos (25 %)")
    pm = prep.fit(m); ms = pm.transform(m).cache()
    res = []
    for k in range(2, 9):
        km = KMeans(k=k, seed=42, maxIter=40).fit(ms)
        res.append({"k": k, "silhouette": round(ev.evaluate(km.transform(ms)), 4), "wssse": round(km.summary.trainingCost, 1)})
        print(f"  k={k}  silhouette={res[-1]['silhouette']:.4f}  WSSSE={res[-1]['wssse']:>12,.1f}", flush=True)
    (RESULTADOS / "seleccion_k.json").write_text(json.dumps(res, indent=1))
else:
    pm = prep.fit(df); d = pm.transform(df)
    km = KMeans(k=K_FINAL, seed=42, maxIter=60).fit(d)
    out = km.transform(d)
    sil = ev.evaluate(out.sample(False, 0.2, seed=1))
    perfil = (out.groupBy("prediction").agg(F.count("*").alias("n"),
              F.expr("percentile_approx(oferentes,0.5)").alias("med_oferentes"), F.avg("oferentes").alias("prom_oferentes"),
              F.avg("oferente_unico").alias("pct_unico"),
              F.expr("percentile_approx(visualizaciones,0.5)").alias("med_vis"),
              F.avg("invitados").alias("prom_invitados"),
              F.expr("percentile_approx(duracion_dias,0.5)").alias("med_dur"),
              F.expr("percentile_approx(valor_adj,0.5)").alias("med_valor"), F.sum("valor_adj").alias("valor_total"))
              .orderBy("prediction"))
    filas = [r.asDict() for r in perfil.collect()]
    centros = [[round(float(x), 3) for x in c] for c in km.clusterCenters()]
    (RESULTADOS / "clusters_perfil.json").write_text(json.dumps({"k": K_FINAL, "silhouette": sil, "features": FEATS, "centros_estandarizados": centros, "perfil": filas}, indent=1, default=str))
    out.select("id_proceso", F.col("prediction").alias("cluster")).write.mode("overwrite").parquet(str(RAIZ / "datos/procesados/asignacion_clusters.parquet"))
    print(f"Modelo final K-Means k={K_FINAL} | procesos: {d.count():,} | silhouette = {sil:.4f}\n")
    print(f"{'cluster':>7} {'procesos':>9} {'med_ofer':>8} {'%_unico':>8} {'med_vis':>7} {'invit':>6} {'med_dias':>8} {'med_valor':>14}")
    for f in filas:
        print(f"{f['prediction']:>7} {f['n']:>9,} {f['med_oferentes']:>8} {f['pct_unico']:>8.1%} {f['med_vis']:>7} {f['prom_invitados']:>6.1f} {f['med_dur']:>8.0f} {f['med_valor']:>14,.0f}")
spark.stop()
