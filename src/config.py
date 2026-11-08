from pathlib import Path
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[1]
BLOQUES = str(RAIZ / "datos" / "procesados" / "bloques")
LIMPIO = str(RAIZ / "datos" / "procesados" / "competitivos_limpio.parquet")
RESULTADOS = RAIZ / "resultados"

# Modalidades donde por ley debe existir competencia entre oferentes
MODALIDADES_COMPETITIVAS = [
    "Mínima cuantía", "Selección Abreviada de Menor Cuantía",
    "Selección abreviada subasta inversa", "Licitación pública",
    "Licitación pública Obra Publica", "Concurso de méritos abierto",
    "Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes",
]

def crear_spark(nombre="SECOP-Competencia"):
    return (SparkSession.builder.appName(nombre).master("local[2]")
            .config("spark.driver.memory", "1600m")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())
