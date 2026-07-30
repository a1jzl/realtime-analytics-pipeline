# Realtime Analytics Pipeline

Architecture de traitement de donnees en temps reel : ingestion de flux d'evenements via Kafka, transformation avec Spark Structured Streaming, stockage dans un data warehouse, et visualisation via un dashboard analytique.

## 1. Contexte et objectif

De nombreuses entreprises ont besoin d'analyser des evenements (transactions, clics, capteurs) au fur et a mesure qu'ils arrivent, plutot que par lots quotidiens. Ce projet simule un flux d'evenements de transactions e-commerce et construit une chaine complete de traitement temps reel, de l'ingestion jusqu'a la visualisation, en respectant les pratiques d'architecture data engineering utilisees en entreprise.

## 2. Architecture

```
Producteur (simulateur d'evenements)
        |
        v
     Kafka (topic: transactions)
        |
        v
Spark Structured Streaming (agregations, enrichissement)
        |
        v
   PostgreSQL (data warehouse)
        |
        v
   Dashboard Streamlit / Metabase
```

Le producteur genere des evenements de transaction (montant, categorie, ville, timestamp) et les publie dans un topic Kafka. Un job Spark Structured Streaming consomme ce topic en continu, applique des agregations par fenetre glissante (chiffre d'affaires par minute, par categorie) et ecrit les resultats dans une base PostgreSQL. Le dashboard interroge cette base pour afficher des indicateurs mis a jour en quasi temps reel.

## 3. Stack technique

- Apache Kafka (via Confluent ou Redpanda en local) pour l'ingestion de flux
- PySpark / Spark Structured Streaming pour le traitement en continu
- PostgreSQL comme data warehouse cible
- Docker Compose pour orchestrer l'ensemble des services localement
- Streamlit pour le dashboard de visualisation

## 4. Structure du repo

```
realtime-analytics-pipeline/
  producer/
    event_generator.py       simulation d'evenements de transaction
  streaming/
    spark_job.py               job Spark Structured Streaming
  warehouse/
    schema.sql                  schema PostgreSQL cible
  dashboard/
    app.py                       dashboard Streamlit
  docker-compose.yml
  requirements.txt
  .github/workflows/ci.yml
```

## 5. Lancement de l'environnement

```bash
docker-compose up -d
python producer/event_generator.py
spark-submit streaming/spark_job.py
streamlit run dashboard/app.py
```

## 6. Indicateurs suivis

Le dashboard presente le chiffre d'affaires cumule par fenetre de temps, le top des categories de produits, et la repartition geographique des transactions, avec un rafraichissement automatique toutes les quelques secondes.

## 7. Limites et pistes d'amelioration

- Le producteur simule des donnees synthetiques ; une integration avec une vraie source (API publique, capteur) rendrait la demo plus realiste.
- La gestion des late events et du watermarking Spark pourrait etre approfondie pour des cas de donnees tres desordonnees.
- Le passage a l'echelle (plusieurs partitions Kafka, cluster Spark) n'est pas demontre dans cette version locale.

## 8. Auteur

Projet realise dans le cadre d'une recherche d'alternance en data/IA.
