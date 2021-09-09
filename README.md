# Data tasks

Ce dépôt contient l'ensemble des tâches de préparation de la donnée nécessaire au lancement de l'application [Envinorma](https://github.com/Envinorma/envinorma-web).

Ces tâches sont décrites [ici](https://envinorma.github.io/architecture/schema_fonctionnel).

Avant d'exécuter ces tâches, cloner ce dépôt, se placer à la racine de celui-ci et préparer les variables d'environnement :

```sh
git clone https://github.com/Envinorma/data-tasks
cd data-tasks
```

## Mettre à jour les classements et les installations à partir de l'extraction S3IC

Pour créer les fichiers CSV des classements et installations utilisés par l'application Envinorma, utiliser au choix docker ou python 3.8. Dans les deux cas, cloner le dépôt comme indiqué ci-dessus puis remplacer dans le script les deux variables suivantes :

- Remplacer `$INPUT_FOLDER` par le chemin vers le dossier contenant les deux fichiers issus de l'extraction DGPR: `AP svelte/s3ic-liste-etablissements.csv` et `AP svelte/sic-liste-rubriques.csv`

- Remplacer `$OUTPUT_FOLDER` par le chemin vers le dossier dans lequel générer les fichiers CSV (`installations_all.csv`, `installations_idf.csv`, `installations_sample.csv`, `classements_all.csv`, `classements_idf.csv`, `classements_sample.csv`)

### 1. Avec docker

```sh
docker build -t tasks .
docker run -it --rm\
  -v $INPUT_FOLDER:/data/secret_data\
  -v $OUTPUT_FOLDER:/data/seeds\
  tasks\
  python3 -m tasks.data_build.generate_data --handle-installations-data
```

### 2. Avec python >= 3.8

```sh
cp default_config.ini config.ini
# Modifier config.ini pour définir storage.seed_folder=$OUTPUT_FOLDER et storage.secret_data_folder=$INPUT_FOLDER
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python3 -m tasks.data_build.generate_data --handle-installations-data
```

## Mettre à jour les fichiers aps{}.csv à partir de l'extraction géorisques

Pour créer le CSV des aps utilisé par l'application Envinorma, utiliser au choix docker ou python 3.8. Dans les deux cas, cloner le dépôt comme indiqué ci-dessus puis remplacer dans le script les deux variables suivantes :

- Remplacer `$INPUT_FOLDER` par le chemin vers le dossier contenant les deux fichiers issus de l'extraction géorisques : `IC_documents.csv` et `IC_types_document.csv`

- Remplacer `$OUTPUT_FOLDER` par le chemin vers le dossier dans lequel générer les fichiers CSV (`aps_all.csv`, `aps_idf.csv`, `aps_sample.csv`). Ce dossier doit contenir les fichiers `installations{}.csv` (cf. paragraphe ci-dessus pour les générer.)

### 1. Avec docker

```sh
docker build -t tasks .
docker run -it --rm\
  -v $INPUT_FOLDER:/data/georisques\
  -v $OUTPUT_FOLDER:/data/seeds\
  tasks\
  python3 -m tasks.data_build.generate_data --handle-aps
```

### 2. Avec python >= 3.8

```sh
cp default_config.ini config.ini
# Modifier config.ini pour définir storage.seed_folder=$OUTPUT_FOLDER et storage.georisques_data_folder=$INPUT_FOLDER
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python3 -m tasks.data_build.generate_data --handle-aps
```

## Faire tourner l'OCR sur les APs dont l'OCR n'a pas été exécuté

Pour ajouter la couche de reconnaissance de caractères sur les AP et les uploader sur OVH, installer docker et remplacer dans le script les deux variables suivantes avant de l'exécuter (après avoir cloné le dépôt comme indiqué ci-dessus) :

- Remplacer `$INPUT_FOLDER` par le chemin vers le dossier contenant les aps : `aps_all.csv`
- Remplacer les valeurs des variables d'environnement FILL_WITH_CORRECT_VALUE par les secrets OVH

```sh
docker build -t ocr -f ocr.dockerfile .
docker run -it --rm\
  -e OS_AUTH_URL="https://auth.cloud.ovh.net/v3/"\
  -e OS_IDENTITY_API_VERSION=3\
  -e OS_USER_DOMAIN_NAME=Default\
  -e OS_PROJECT_DOMAIN_NAME=Default\
  -e OS_TENANT_ID=FILL_WITH_CORRECT_VALUE\
  -e OS_TENANT_NAME=FILL_WITH_CORRECT_VALUE\
  -e OS_USERNAME=FILL_WITH_CORRECT_VALUE\
  -e OS_PASSWORD=FILL_WITH_CORRECT_VALUE\
  -e OS_REGION_NAME=SBG\
  -v /data/seeds=$INPUT\
  ocr
```

Puis reconstruire les fichier aps.csv à partir du paragraphe ci-dessus `Mettre à jour les fichiers aps[...]`

## Générer la nomenclature `classement_references.csv` à partir de l'extraction géorisques

Le fichier CSV contenant les références de classements peut être généré avec python>=3.8 ou docker (cf. scripts ci-dessous). Dans les deux cas, après avoir cloné le dépôt comme indiqué ci-dessus, remplacer les variables suivantes :

- Remplacer `$INPUT_FOLDER` par le chemin vers le dossier contenant le fichier issus de l'extraction géorisques : `IC_ref_nomenclature_ic.csv`

- Remplacer `$OUTPUT_FOLDER` par le chemin vers le dossier dans lequel générer les fichier CSV `classement_references.csv`

### 1. Avec docker

```sh
docker build -t tasks .
docker run -it --rm\
  -v $INPUT_FOLDER:/data/georisques\
  -v $OUTPUT_FOLDER:/data/seeds\
  tasks\
  python3 scripts/generate_classement_references.py
```

### 2. Avec python >= 3.8

```sh
cp default_config.ini config.ini
# Modifier config.ini pour définir storage.seed_folder=$OUTPUT_FOLDER et storage.georisques_data_folder=$INPUT_FOLDER
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/generate_classement_references.py
```

## Planifier et exécuter les tâches avec Prefect

Les tâches définies dans `tasks/flows.py` peuvent être exécutées dans le même environnement et orchestrées par [Prefect](http://prefect.io).
Pour exécuter l'agent qui planifie et exécute à son tour les tâches :

```sh
cp default_config.ini config.ini # Renseigner toutes les variables sans valeur par défaut
docker build -t tasks .
docker run -it --rm tasks
```
