"""
Camada Bronze - Ingestão de dumps CSV do Postgres
==================================================

Este arquivo faz parte da pipeline Lakeflow ETL "pipeline_medalhao_etl"
e deve ser salvo dentro de: transformations/bronze/bronze_ingestao.py

Lê os 6 arquivos CSV localizados no volume:
    /Volumes/workspace/bronze/landing/


Cada tabela bronze recebe também duas colunas de metadados:
    _ingestion_timestamp -> quando o registro foi processado por esta pipeline
    _source_file          -> nome do arquivo CSV de origem
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

# ---------------------------------------------------------------------------
# 1) Configuração: mapeie aqui o nome de cada arquivo CSV (dentro do volume
#    "landing") para o nome que a tabela bronze correspondente deve receber.
# ---------------------------------------------------------------------------
ARQUIVOS_BRONZE = {
    "school_groups.csv":     "grupos_raw",
    "schools.csv":     "escolas_raw",
    "grades.csv":      "series_raw",
    "classrooms.csv": "classes_raw",
    "activities.csv":   "atividades_raw",
    "activity_users.csv":    "atividade_alunos_raw",
}

VOLUME_LANDING = "/Volumes/workspace/bronze/landing"

# Onde o Auto Loader guarda o schema (nomes de colunas) que ele descobre em
# cada arquivo. Fica numa subpasta oculta dentro do próprio volume "landing".
SCHEMA_LOCATION_BASE = f"{VOLUME_LANDING}/_autoloader_schemas"


# ---------------------------------------------------------------------------
# 2) Função-fábrica: gera uma tabela bronze para um arquivo CSV específico.
#    Usar uma função-fábrica (em vez de definir a @dp.table direto dentro
#    do "for") para criar várias tabelas em loop sem cair no problema de 
#    "late binding" de variáveis em closures do Python.
# ---------------------------------------------------------------------------
def gerar_tabela_bronze(nome_arquivo: str, nome_tabela: str):
    
    @dp.table(
        name=f"workspace.bronze.{nome_tabela}",
        comment=(
            f"Dump bruto da tabela Postgres '{nome_tabela}', ingerido a "
            f"partir de '{nome_arquivo}'. Todas as colunas mantidas como "
            f"STRING nesta camada (inclui campos json, jsonb, array e "
            f"tsvector do Postgres como texto puro)."
        ),
    )
    def _ingestao():
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            # Mantém TODAS as colunas como string — comportamento padrão do
            # Auto Loader para CSV/JSON, mas deixado explícito de propósito.
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/{nome_tabela}")
            .option("header", "true")
            # RFC4180 / padrão do "COPY ... CSV" do Postgres: aspas duplas
            # duplicadas ("") dentro de um campo escapam uma aspa literal.
            # Essencial para campos jsonb/array, que costumam conter aspas
            # e vírgulas dentro do próprio valor.
            .option("quote", '"')
            .option("escape", '"')
            # Um json/jsonb "pretty printed" pode conter quebras de linha
            # dentro do campo. multiLine evita que a linha seja cortada no
            # meio. Reduz um pouco o paralelismo de leitura de cada arquivo,
            # mas para arquivos de dump isso não costuma ser um problema.
            .option("multiLine", "true")
            .option("pathGlobFilter", nome_arquivo)
            .load(VOLUME_LANDING)
        )

        # Reforço explícito: garante que absolutamente todas as colunas
        # fiquem como STRING, mesmo que a opção acima mude de comportamento
        # no futuro.
        df = df.select(*[col(c).cast("string").alias(c) for c in df.columns])

        return df.select(
            "*",
            current_timestamp().alias("_ingestion_timestamp"),
            col("_metadata.file_name").alias("_source_file"),
        )

    return _ingestao


# ---------------------------------------------------------------------------
# 3) Gera as 6 tabelas bronze dinamicamente a partir do dicionário acima.
# ---------------------------------------------------------------------------
for arquivo, tabela in ARQUIVOS_BRONZE.items():
    gerar_tabela_bronze(arquivo, tabela)