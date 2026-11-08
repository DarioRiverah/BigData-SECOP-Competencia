"""Etapa 1 - Ingesta por bloques del CSV crudo de SECOP II (~10 GB).
Lee el archivo en streaming, conserva solo las columnas del contexto de
COMPETENCIA y escribe bloques Parquet. Guarda el byte de avance para poder
reanudar (útil en equipos con poca RAM / sesiones con límite de tiempo)."""
import csv, io, json, sys, time
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq

RAIZ = Path(__file__).resolve().parents[1]
CSV = RAIZ.parent / "SecopII_ProcesosDeContratacion.csv"
DEST = RAIZ / "datos" / "procesados" / "bloques"
ESTADO = RAIZ / "datos" / "procesados" / "estado_ingesta.json"
LIMITE_SEG = float(sys.argv[1]) if len(sys.argv) > 1 else 1e9
FILAS_BLOQUE = 400_000

COLUMNAS = {
    "ID del Proceso": "id_proceso", "Entidad": "entidad",
    "Departamento Entidad": "depto_entidad", "OrdenEntidad": "orden_entidad",
    "Entidad Centralizada": "centralizada",
    "Fecha de Publicacion del Proceso": "fecha_publicacion",
    "Modalidad de Contratacion": "modalidad", "Duracion": "duracion",
    "Unidad de Duracion": "unidad_duracion",
    "Proveedores Invitados": "prov_invitados",
    "Visualizaciones del Procedimiento": "visualizaciones",
    "Proveedores que Manifestaron Interes": "prov_interes",
    "Respuestas al Procedimiento": "respuestas",
    "Proveedores Unicos con Respuestas": "oferentes_unicos",
    "Numero de Lotes": "num_lotes", "Estado del Procedimiento": "estado",
    "Adjudicado": "adjudicado", "Departamento Proveedor": "depto_proveedor",
    "Valor Total Adjudicacion": "valor_adjudicado",
    "Codigo Principal de Categoria": "codigo_categoria",
    "Fecha Adjudicacion": "fecha_adjudicacion",
}

DEST.mkdir(parents=True, exist_ok=True)
est = json.loads(ESTADO.read_text()) if ESTADO.exists() else {"offset": 0, "bloque": 0, "filas": 0, "malas": 0, "fin": False}
if est["fin"]:
    print("Ingesta ya terminada", est); sys.exit()

f = open(CSV, "rb")
pos = [est["offset"]]
if pos[0] == 0:
    enc = next(csv.reader([f.readline().decode("utf-8-sig")]))
    pos[0] = f.tell(); est["header"] = enc
else:
    f.seek(pos[0])
enc = est["header"]
idx = [enc.index(c) for c in COLUMNAS]
nombres = list(COLUMNAS.values())

def lineas():
    for ln in f:
        pos[0] += len(ln)
        yield ln.decode("utf-8", errors="replace")

print(f"Archivo: {CSV.name} ({CSV.stat().st_size/1e9:.2f} GB) | columnas originales: {len(enc)} | seleccionadas: {len(idx)}")
print(f"Reanudando desde el byte {pos[0]:,}" if est['offset'] else "Iniciando lectura por bloques de 400.000 filas...")
lector = csv.reader(lineas())
t0 = time.time()
buf = [[] for _ in nombres]
n = 0

def volcar():
    global buf, n
    if n == 0: return
    tabla = pa.table({k: pa.array(v, pa.string()) for k, v in zip(nombres, buf)})
    pq.write_table(tabla, DEST / f"bloque_{est['bloque']:04d}.parquet", compression="zstd")
    est["bloque"] += 1; est["filas"] += n; est["offset"] = pos[0]
    ESTADO.write_text(json.dumps(est))
    print(f"  bloque_{est['bloque']-1:04d}.parquet  filas={est['filas']:>10,}  leído={pos[0]/1e9:5.2f} GB  t={time.time()-t0:5.0f}s", flush=True)
    buf = [[] for _ in nombres]; n = 0

ncols = len(enc)
for fila in lector:
    if len(fila) != ncols:
        est["malas"] += 1; continue
    for j, i in enumerate(idx):
        buf[j].append(fila[i] or None)
    n += 1
    if n >= FILAS_BLOQUE:
        volcar()
        if time.time() - t0 > LIMITE_SEG: break
else:
    volcar(); est["fin"] = True; ESTADO.write_text(json.dumps(est))

print(f"\nResumen: {est['bloque']} bloques Parquet | {est['filas']:,} filas | {est['malas']} filas mal formadas | "
      + ("INGESTA COMPLETA" if est['fin'] else "pausado, volver a ejecutar para reanudar") + f" | {time.time()-t0:.0f} s")
