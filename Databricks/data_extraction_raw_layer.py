# Databricks notebook source
#IMPORTANDO BIBLIOTECAS

import os
import requests
import json

# COMMAND ----------

#DEFININDO VARIAVÉIS PARA CONECTAR AO DATALAKE

datalake_name = dbutils.secrets.get(scope="spotify", key="datalake-name")
datalake_key = dbutils.secrets.get(scope="spotify", key="datalake-key")
container_name = dbutils.secrets.get(scope="spotify", key="container-name")

# COMMAND ----------

#CONEXÃO COM O DATALAKE

if not any(mount.mountPoint == "/mnt/bronze" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{datalake_name}.blob.core.windows.net/bronze",
        mount_point="/mnt/bronze",
        extra_configs={
            f"fs.azure.account.key.{datalake_name}.blob.core.windows.net": datalake_key
        }
    )
else:
    print("A montagem '/mnt/bronze' já existe.")

# COMMAND ----------

#OBTENDO AS CREDENCIAIS PARA GERAR TOKEN

client_id = dbutils.secrets.get(scope="spotify", key="client-id")
client_secret = dbutils.secrets.get(scope="spotify", key="client-secret")

# COMMAND ----------

#REQUISIÇÃO PARA OBTER O TOKEN DA API DO SPOTIFY

url_token = "https://accounts.spotify.com/api/token"

headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret
}

response = requests.post(url_token, headers=headers, data=data)

if response.status_code == 200:
    token_info = response.json()
    access_token = token_info['access_token']
    print(f"Token de acesso obtido com sucesso.")
else:
    print(f"Erro ao obter token: {response.status_code} - {response.text}")

# COMMAND ----------

#REQUISIÇÃO PARA OBTER ID DO ARTISTA

url_search = "https://api.spotify.com/v1/search"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

artists = [
    'Billie Eilish',
    'Adele',
    'The Weeknd',
    'Beyoncé',
    'Bruno Mars',
    'Post Malone',
    'Shawn Mendes',
    'Taylor Swift',
    'Harry Styles',
    'Freddie Mercury',
    'Nirvana',
    'Eminem',
    'Ed Sheeran',
    'Kendrick Lamar',
    'Dua Lipa',
    'Lady Gaga',
    'The Rolling Stones',
    'Rihanna',
    'Imagine Dragons',
    'Coldplay',
    'Maroon 5'
]

artist_info = []

for artist in artists:
    
    params = {
        'q': artist,
        'type': 'artist'
    }

    response = requests.get(url_search, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        if data['artists']['items']:
            artist_info.append(data['artists']['items'][0])  
            print(f"Artista {artist} encontrado.")
    else:
        print(f"Erro ao buscar artista {artist}: {response.status_code}")


# COMMAND ----------

# REQUISIÇÃO PARA OBTER INFORMAÇÕES DO ARTISTA

url_artist = "https://api.spotify.com/v1/artists/{}"

artist_details_data = {}

for artist in artist_info:
    artist_id = artist['id']
    
    url_artist_formatted = url_artist.format(artist_id)
    artist_response = requests.get(url_artist_formatted, headers=headers)

    if artist_response.status_code == 200:
        artist_details = artist_response.json()
        artist_details_data[artist['name']] = artist_details
        print(f"Detalhes de {artist_details['name']} coletados com sucesso.")
    else:
        print(f"Erro ao obter informações do artista {artist_id}: {artist_response.status_code}")

# COMMAND ----------

#REQUISIÇÃO PARA OBTER PRINCIPAIS MÚSICAS DO ARTISTA
url_artist_top_tracks = "https://api.spotify.com/v1/artists/{}/top-tracks?market=BR"

artist_tracks_data = {}

for artist in artist_info:
    artist_id = artist['id']

    artist_tracks_data[artist['name']] = {}
    
    top_tracks_url = url_artist_top_tracks.format(artist_id)
    top_tracks_response = requests.get(top_tracks_url, headers=headers)
    
    if top_tracks_response.status_code == 200:
        top_tracks = top_tracks_response.json()
        artist_tracks_data[artist['name']]= top_tracks['tracks']
        print(f"Principais músicas de {artist['name']} obtidas com sucesso.")
    else:
        print(f"Erro ao obter músicas do artista {artist['name']}: {top_tracks_response.status_code}")



# COMMAND ----------

# SALVAR INFORMAÇÕES NA CAMADA BRONZE
base_path = "/mnt/bronze/"  
os.makedirs(base_path, exist_ok=True)

for artist_name in artist_details_data.keys():

    formatted_artist_name = artist_name.replace(" ", "_").lower() + ".json" 

    artist_data = {
        'artist_details': artist_details_data[artist_name],
        'top_tracks': artist_tracks_data.get(artist_name, {})
    }
    
    file_path = os.path.join(base_path, formatted_artist_name)
    
    try:
        dbutils.fs.put(file_path, json.dumps(artist_data), overwrite=True)
        print(f"Dados de {artist_name} salvos com sucesso em {formatted_artist_name}.")
    except Exception as e:
        print(f"Erro ao salvar os dados de {artist_name}: {e}")

