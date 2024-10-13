# Databricks notebook source
#IMPORTANDO BIBLIOTECAS

from pyspark.sql.functions import col
from pyspark.sql import functions as F

# COMMAND ----------

#DEFININDO VARIAVÉIS PARA CONECTAR AO DATALAKE

datalake_name = dbutils.secrets.get(scope="spotify", key="datalake-name")
datalake_key = dbutils.secrets.get(scope="spotify", key="datalake-key")
container_name = dbutils.secrets.get(scope="spotify", key="container-name")

# COMMAND ----------

#CONEXÃO COM O DATALAKE

if not any(mount.mountPoint == "/mnt/ouro" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{datalake_name}.blob.core.windows.net/ouro",
        mount_point="/mnt/ouro",
        extra_configs={
            f"fs.azure.account.key.{datalake_name}.blob.core.windows.net": datalake_key
        }
    )
else:
    print("A montagem '/mnt/ouro' já existe.")

# COMMAND ----------

#EXTRAINDO DADOS TRATADOS DA CAMADA PRATA

df_fact_songs = spark.read.parquet("/mnt/prata/fact_songs")

df_dim_artists = spark.read.parquet("/mnt/prata/dim_artists")

df_dim_albuns = spark.read.parquet("/mnt/prata/dim_albuns")


# COMMAND ----------


view_df = df_fact_songs.alias("songs") \
    .join(df_dim_artists.alias("artists"), col("songs.artist_id") == col("artists.artist_id"), "inner") \
    .join(df_dim_albuns.alias("albums"), col("songs.album_id") == col("albums.album_id"), "inner") \
    .groupBy(
        col("songs.song_id"),
        col("songs.artist_id"),
        col("songs.album_id"),
        col("songs.song_name"),
        col("songs.duration").alias("song_duration"),
        col("songs.popularity").alias("song_popularity"),
        col("artists.artist_name"),
        col("artists.genre"),
        col("artists.popularity").alias("artist_popularity"),
        col("artists.followers").alias("artist_followers"),
        col("artists.artist_image"),
        col("albums.album_name"),
        col("albums.album_release_date"),
        col("albums.album_type"),
        col("albums.album_image")
    ) 
view_df.display()

# COMMAND ----------

#SALVANDO NA CAMADA OURO EM PARQUET

path = "/mnt/ouro/"

view_df.write.mode("overwrite").parquet(path)
