# Data tasks

Ce dépôt contient l'ensemble des tâches de préparation de la donnée nécessaire au lancement de l'application [Envinorma](https://github.com/Envinorma/envinorma-web).

Ces tâches sont décrites [ici](https://envinorma.github.io/architecture).

Avant d'exécuter ces tâches, cloner ce dépôt, se placer à la racine de celui-ci et préparer les variables d'environnement :

```sh
git clone https://github.com/Envinorma/data-tasks
cd data-tasks
```

## Scripts contenus dans ce dépôt

- Mettre à jour les classements et les installations à partir de l'extraction S3IC
- Mettre à jour les CSV contenant les métadonnées des AP à partir de l'extraction géorisques
- Faire tourner l'OCR sur les APs dont l'OCR n'a pas déjà été exécuté
- Générer la nomenclature à partir de l'extraction géorisques

Ces scripts sont documentés [ici](https://envinorma.github.io/data).

## Planifier et exécuter les tâches avec Prefect

> NB : ce n'est pas par ce moyen que sont exécutées les tâches actuellement.

Les tâches définies dans `tasks/flows.py` peuvent être exécutées dans le même environnement et orchestrées par [Prefect](http://prefect.io).
Pour exécuter l'agent qui planifie et exécute à son tour les tâches :

```sh
cp default_config.ini config.ini # Renseigner toutes les variables sans valeur par défaut
docker build -t tasks .
docker run -it --rm tasks
```
