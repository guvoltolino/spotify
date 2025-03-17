
# 🎧Spotify Artists Analysis 🎶

Este projeto tem como objetivo realizar um processo completo de ETL (Extração, Transformação e Carga) utilizando a API do Spotify, armazenar os dados processados no Azure Data Lake e visualizá-los no Power BI. A pipeline ETL foi implementada no Databricks utilizando PySpark para manipulação dos dados.

## 🌟 Funcionalidades

O projeto extrai dados de músicas, álbuns e artistas diretamente da API do Spotify. Após a extração, os dados são transformados e armazenados no Azure Data Lake. Ao final do processo, uma tabela com informações consolidada sobre músicas, artistas e álbuns é gerada para ser utilizada em relatórios no Power BI.

### 📊 Colunas Finais

A tabela final contém as seguintes colunas:

- **song_id**: Identificador único da música.
- **artist_id**: Identificador único do artista.
- **album_id**: Identificador único do álbum.
- **song_name**: Nome da música.
- **song_duration**: Duração da música.
- **song_popularity**: Popularidade da música no Spotify.
- **artist_name**: Nome do artista.
- **artist_genres**: Gêneros musicais associados ao artista.
- **artist_popularity**: Popularidade do artista no Spotify.
- **artist_followers**: Quantidade de seguidores do artista.
- **artist_image**: Imagem do artista.
- **album_name**: Nome do álbum.
- **album_release_date**: Data de lançamento do álbum.
- **album_type**: Tipo de álbum (álbum, single, etc.).
- **album_image**: Imagem do álbum.

## 🛠️ Tecnologias Utilizadas

- **Databricks**: Plataforma de análise de dados utilizada para desenvolver o pipeline ETL.
- **PySpark**: Framework para processamento distribuído de grandes volumes de dados.
- **API do Spotify**: Fonte de dados utilizada para extrair informações sobre músicas, álbuns e artistas.
- **Azure Data Lake**: Armazenamento dos dados processados.
- **Power BI**: Ferramenta de visualização de dados para criação de relatórios e dashboards.

## 🗂 Estrutura do Projeto

O projeto está organizado em três etapas principais:

1. **Extração**: Coleta dos dados da API do Spotify.
2. **Transformação**: Limpeza e transformação dos dados utilizando PySpark, incluindo a criação de tabelas de dimensões e fatos.
3. **Carga**: Armazenamento dos dados transformados no Azure Data Lake.

## ✨ Modelagem de Dados - Star Schema

O projeto utiliza a modelagem **Star Schema** (Esquema em Estrela), que é uma estrutura comumente utilizada em data warehouses. Ele consiste em uma tabela fato central, neste caso a `Fact_Songs`, que armazena os dados principais, e várias tabelas de dimensões ao redor, como `Dim_Artists` e `Dim_Albums`, que armazenam os atributos descritivos relacionados às músicas, artistas e álbuns. As tabelas de dimensões são conectadas à tabela fato por chaves estrangeiras.

Essa modelagem facilita a consulta e visualização de dados, tornando-a ideal para ser utilizada no Power BI.

![image](https://github.com/user-attachments/assets/7fe39226-d0e6-40e4-96a0-ad4a8a873e3f)

## 📊 Visualização no Power BI

Os dados finais armazenados no Azure Data Lake são consumidos pelo Power BI para gerar dashboards interativos com informações detalhadas sobre músicas, artistas e álbuns.

![image](https://github.com/user-attachments/assets/80528e42-b351-40b1-aff9-fa61bc58c671)


