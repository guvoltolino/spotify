# Databricks notebook source
#IMPORTANDO BIBLIOTECAS

import os
import json
from pyspark.sql.functions import explode, expr, col

# COMMAND ----------

#DEFININDO VARIAVÉIS PARA CONECTAR AO DATALAKE

datalake_name = dbutils.secrets.get(scope="spotify", key="datalake-name")
datalake_key = dbutils.secrets.get(scope="spotify", key="datalake-key")
container_name = dbutils.secrets.get(scope="spotify", key="container-name")

# COMMAND ----------

#CONEXÃO COM O DATALAKE

if not any(mount.mountPoint == "/mnt/prata" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{datalake_name}.blob.core.windows.net/prata",
        mount_point="/mnt/prata",
        extra_configs={
            f"fs.azure.account.key.{datalake_name}.blob.core.windows.net": datalake_key
        }
    )
else:
    print("A montagem '/mnt/prata' já existe.")


# COMMAND ----------

#LER OS ARQUIVOS JSON

bronze_path = "/mnt/bronze/"

df = spark.read.json(bronze_path)


# COMMAND ----------

dim_artists_df = df.selectExpr(
    "artist_details.id as artist_id",
    "artist_details.name as artist_name",
    "artist_details.popularity as popularity",
    "artist_details.followers.total as followers",
    "artist_details.images.url[0] as artist_image",
    "artist_details.genres[0] as genre"  
)

#SALVANDO NA CAMADA PRATA EM PARQUET

path = "/mnt/prata/dim_artists"

dim_artists_df.write.mode("overwrite").parquet(path)

dim_artists_df.display()

# COMMAND ----------

dim_albuns_df = df.select(explode("top_tracks").alias("track")) \
    .selectExpr(
        "track.album.id as album_id",                      
        "track.album.name as album_name",              
        "track.album.release_date as album_release_date", 
        "track.album.album_type as album_type",        
        "track.album.images[0].url as album_image",    
        "explode(track.artists.id) as artist_id"             
    )

#SALVANDO NA CAMADA PRATA EM PARQUET

path = "/mnt/prata/dim_albuns"

dim_albuns_df.write.mode("overwrite").parquet(path)

dim_albuns_df.display()

# COMMAND ----------

fact_songs_df = df.select(explode("top_tracks").alias("track")) \
    .select(
        col("track.id").alias("song_id"),                  
        explode("track.artists").alias("artist"),           
        col("track.album.id").alias("album_id"),            
        col("track.name").alias("song_name"),              
        col("track.duration_ms").alias("duration"),          
        col("track.popularity").alias("popularity")          
    ) \
    .select( 
        "song_id",
        col("artist.id").alias("artist_id"),                
        "album_id",
        "song_name",
        "duration",
        "popularity"
    )

fact_songs_df = fact_songs_df.distinct()

#SALVANDO NA CAMADA PRATA EM PARQUET

path = "/mnt/prata/fact_songs"

fact_songs_df.write.mode("overwrite").parquet(path)

fact_songs_df.display()
