# MLflow Preprocessing Runner

Système pour exécuter des preprocessings depuis MLflow dans des containers Docker.

## Principe

1. **MLflow** stocke le code de preprocessing
2. **Build Docker** télécharge le preprocessing depuis MLflow
3. **Container** exécute le preprocessing
4. **Kafka** reçoit les données prétraitées

## Structure

```
mlflow-preprocessing-runner/
├── runner/
│   └── runner.py              # Exécute pre_processing()
├── docker/
│   ├── Dockerfile             # Image générique
│   └── entrypoint.sh          # Script d'entrée
├── build_scripts/
│   └── build_image.py         # Build Docker + download MLflow
├── preprocessing/             # (vide - rempli au build-time)
└── examples/
    └── simple_preprocessing.py  # Exemple
```

## Utilisation

### 1. Préparer le preprocessing dans MLflow

Uploader dans MLflow :
- `preprocessing.py` : Code avec fonction `pre_processing(data)`
- `requirements.txt` (optionnel) : Dépendances Python pour le preprocessing

**Note** : Si votre preprocessing a besoin de dépendances (pandas, sklearn, etc.),
créez un fichier `requirements.txt` et uploadez-le dans MLflow avec `preprocessing.py`.

### 2. Build l'image Docker

```bash
# Installer les dépendances
pip install -r requirements.txt

# Configurer MLflow
export MLFLOW_TRACKING_URI="http://127.0.0.1:5000"

# Build basique (utilise le tag "latest" par défaut)
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1

# Build avec tag spécifique
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1 --tag v1.0.0

# Build avec preprocessing dans un sous-dossier MLflow (ex: code/preprocessing.py)
# Tous les fichiers Python du dossier seront copiés automatiquement (processor.py, __init__.py, etc.)
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1 \
    --preprocessing-path code/preprocessing.py

# Build avec Dockerfile personnalisé
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1 \
    --dockerfile docker/custom.Dockerfile

# Build avec version Python spécifique (si requirements nécessitent Python >=3.11)
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1 \
    --python-version 3.11

# Voir toutes les options et exemples
python build_scripts/build_image.py --help
```

### 3. Exécuter le container

```bash
# JSONL depuis stdin
echo '{"test": "data"}' | docker run -i preprocessing-model-v1:latest

# Fichier JSON
docker run -i preprocessing-model-v1:latest input.json > output.jsonl

# Fichier Avro
docker run -i preprocessing-model-v1:latest input.avro > output.jsonl
```

## Format du preprocessing

Le preprocessing doit avoir une fonction `pre_processing(data)` :

```python
def pre_processing(data):
    # Votre logique
    return processed_data
```

## Formats d'entrée supportés

- **JSONL** : Une ligne JSON par record
- **JSON** : Objet ou tableau JSON complet
- **Avro** : Fichiers Avro binaires

## Format de sortie

JSONL (une ligne JSON par record transformé).
