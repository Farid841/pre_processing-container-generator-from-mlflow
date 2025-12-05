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

#### Option 1 : Auto-build (recommandé)

Le nom d'image est généré automatiquement depuis les métadonnées MLflow :
- Format : `{type}-{model_name}-{version}`
- Exemple : `preprocessing-model-v1`

```bash
# Installer les dépendances
pip install -r requirements.txt

# Configurer MLflow
export MLFLOW_TRACKING_URI="http://127.0.0.1:5000"

# Auto-build avec script shell (le plus simple)
./build.sh e6c1131f4673449aa688ed1ffc3abbbe

# Auto-build avec tag spécifique
./build.sh e6c1131f4673449aa688ed1ffc3abbbe --tag v1.0.0

# Auto-build avec preprocessing dans un sous-dossier
./build.sh e6c1131f4673449aa688ed1ffc3abbbe --preprocessing-path code/preprocessing.py

# Auto-build avec Python 3.11
./build.sh e6c1131f4673449aa688ed1ffc3abbbe --python-version 3.11
```

#### Option 2 : Build manuel avec Python

```bash
# Build basique avec nom d'image auto-généré
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe auto

# Build avec nom d'image manuel
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1

# Build avec tag spécifique
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe auto --tag v1.0.0

# Build avec preprocessing dans un sous-dossier MLflow (ex: code/preprocessing.py)
# Tous les fichiers Python du dossier seront copiés automatiquement (processor.py, __init__.py, etc.)
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe auto \
    --preprocessing-path code/preprocessing.py

# Build avec Dockerfile personnalisé
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe auto \
    --dockerfile docker/custom.Dockerfile

# Build avec version Python spécifique (si requirements nécessitent Python >=3.11)
python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe auto \
    --python-version 3.11

# Voir toutes les options et exemples
python build_scripts/build_image.py --help
```

### 3. Exécuter le container

#### Mode stdout (par défaut)

```bash
# JSONL depuis stdin
echo '{"test": "data"}' | docker run -i preprocessing-model-v1:latest

# Fichier JSON
docker run -i preprocessing-model-v1:latest input.json > output.jsonl

# Fichier Avro
docker run -i preprocessing-model-v1:latest input.avro > output.jsonl
```

#### Mode Kafka

**Prérequis** : Kafka doit être démarré (voir section Kafka ci-dessous)

```bash
# Avec fichier .env.docker (recommandé)
echo '{"test": "data"}' | docker run -i --env-file .env.docker preprocessing-model-v1:latest

# Avec script automatique
echo '{"test": "data"}' | ./run_with_kafka.sh preprocessing-model-v1:latest

# Variables manuelles
echo '{"test": "data"}' | docker run -i \
  -e KAFKA_ENABLED=true \
  -e KAFKA_BOOTSTRAP_SERVERS=172.17.0.1:29092 \
  -e KAFKA_TOPIC_SUFFIX=cleaned \
  preprocessing-model-v1:latest
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

## Configuration Kafka

Le preprocessing peut envoyer les résultats vers Kafka ou stdout (par défaut).

### Démarrer Kafka

```bash
# Démarrer Kafka avec docker-compose
docker compose -f docker-compose.kafka.yml up -d

# Vérifier que Kafka est démarré
docker ps | grep kafka
```

### Variables d'environnement

**Pour containers Docker** (fichier `.env.docker`) :
```bash
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=172.17.0.1:29092
KAFKA_TOPIC_SUFFIX=cleaned
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

**Pour accès depuis l'hôte** (fichier `.env`) :
```bash
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC_SUFFIX=cleaned
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

### Vérifier les messages dans Kafka

```bash
# Utiliser le script de vérification
./check_kafka_topic.sh test-model-v1.0.0-cleaned

# Ou utiliser Kafka UI
# Ouvrir http://localhost:8085 dans le navigateur
```

Le topic Kafka sera automatiquement : `{model_name}-{version}-{suffix}` (ex: `test-model-v1.0.0-cleaned`)

### Tags MLflow recommandés

Pour que le système récupère automatiquement les informations, ajoutez ces tags dans MLflow :
- `model_name` : Nom du modèle (ex: "model")
- `version` : Version (ex: "v1", "1.0.0")
- `type` : Type de composant ("preprocessing" ou "model")

## Setup MLflow local (pour tests)

Pour créer un MLflow local avec un preprocessing d'exemple :

```bash
# Démarrer MLflow (dans un terminal séparé)
mlflow ui --backend-store-uri file://./mlruns --host 127.0.0.1 --port 5000

# Créer un run avec preprocessing d'exemple
python setup_mlflow_local.py
```

Le script crée automatiquement :
- Un experiment "preprocessing-runs"
- Un run avec les tags requis
- Les artifacts `preprocessing.py` et `requirements.txt`
- Un modèle scikit-learn entraîné avec métriques

Le `run_id` sera affiché à la fin pour tester le build.
