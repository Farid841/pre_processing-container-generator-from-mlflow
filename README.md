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
│   ├── build_image.py         # Build Docker + download MLflow
│   └── api.py                 # API REST pour les builds
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

#### Mode API REST (recommandé)

Le container expose maintenant une API REST au lieu de stdin/stdout.

```bash
# Démarrer le container
docker run -d -p 8000:8000 --name preprocessing-api preprocessing-model-v1:latest

# Préprocesser un seul record
curl -X POST http://localhost:8000/preprocess \
  -H "Content-Type: application/json" \
  -d '{"data": {"test": "data", "value": 123}}'

# Préprocesser plusieurs records (batch)
curl -X POST http://localhost:8000/preprocess/batch \
  -H "Content-Type: application/json" \
  -d '{"data": [{"test": "data1"}, {"test": "data2"}]}'

# Vérifier la santé
curl http://localhost:8000/health

# Documentation interactive
# Ouvrir http://localhost:8000/docs dans le navigateur
```

**Port personnalisé** :
```bash
docker run -d -p 9000:9000 -e API_PORT=9000 preprocessing-model-v1:latest
```

#### Mode stdout (ancien, toujours disponible)

Si vous préférez l'ancien mode stdin/stdout, vous pouvez toujours utiliser le runner directement :

```bash
# JSONL depuis stdin
echo '{"test": "data"}' | docker run -i preprocessing-model-v1:latest python /app/runner/runner.py

# Fichier JSON
docker run -i preprocessing-model-v1:latest python /app/runner/runner.py input.json > output.jsonl

# Fichier Avro
docker run -i preprocessing-model-v1:latest python /app/runner/runner.py input.avro > output.jsonl
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

### Via API REST (mode par défaut)
- **JSON** : Objet JSON simple pour `/preprocess`
- **JSON Array** : Tableau JSON pour `/preprocess/batch`

### Via stdin/stdout (mode legacy)
- **JSONL** : Une ligne JSON par record
- **JSON** : Objet ou tableau JSON complet
- **Avro** : Fichiers Avro binaires

## Format de sortie

### Via API REST
- **JSON** : Objet JSON avec le résultat préprocessé
- **JSON Array** : Tableau JSON pour les batch requests

### Via stdout (mode legacy)
- **JSONL** : Une ligne JSON par record transformé

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

## Pipeline Kafka Complet

Le système supporte un pipeline Kafka complet avec intégration directe dans le preprocessing et utilisation du Kafka Bridge pour le model.

### Architecture

```
fink-alert (Kafka Topic)
    ↓
Preprocessing Container (Consumer Kafka + API + Producer Kafka)
    ↓
preprocessed (Kafka Topic)
    ↓
Kafka Bridge (sidecar)
    ↓
Model Container (API MLflow)
    ↓
Kafka Bridge (sidecar)
    ↓
predictions (Kafka Topic)
```

### 1. Preprocessing avec Kafka intégré

Le preprocessing container peut consommer directement depuis Kafka et produire vers un autre topic.

#### Variables d'environnement pour Preprocessing

```bash
# Activer Kafka
KAFKA_ENABLED=true

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
INPUT_TOPIC=fink-alert
OUTPUT_TOPIC=preprocessed

# Formats
INPUT_FORMAT=avro  # ou json
OUTPUT_FORMAT=json  # ou avro

# Consumer
CONSUMER_GROUP_ID=preprocessing-group
AUTO_OFFSET_RESET=earliest

# Schéma Avro (si INPUT_FORMAT=avro)
AVRO_SCHEMA_PATH=/app/schemas/ztf_alert_v3.3.avsc

# Options
SKIP_CUTOUTS=true  # Retirer les cutouts pour réduire la taille
```

#### Démarrage du Preprocessing avec Kafka

```bash
docker run -d \
  --name preprocessing \
  -e KAFKA_ENABLED=true \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e INPUT_TOPIC=fink-alert \
  -e OUTPUT_TOPIC=preprocessed \
  -e INPUT_FORMAT=avro \
  -e OUTPUT_FORMAT=json \
  preprocessing-remote-lr-0-1-5af69ee1:latest
```

Le container démarre automatiquement :
- L'API REST sur le port 8000 (toujours accessible)
- Le Kafka processor qui consomme depuis `fink-alert` et produit vers `preprocessed`

### 2. Model avec Kafka Bridge

Pour le model, utilisez le Kafka Bridge existant comme sidecar.

#### Construire l'image Kafka Bridge

```bash
docker build -f docker/Dockerfile.bridge -t kafka-bridge:latest .
```

#### Variables d'environnement pour Model Bridge

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
INPUT_TOPIC=preprocessed
OUTPUT_TOPIC=predictions

# API Model
API_URL=http://model-container:8080
API_ENDPOINT=/invocations

# Formats
INPUT_FORMAT=json
OUTPUT_FORMAT=json

# Consumer
CONSUMER_GROUP_ID=model-bridge-group
AUTO_OFFSET_RESET=earliest

# Batching
BATCH_SIZE=10
BATCH_TIMEOUT_MS=1000
```

#### Démarrage du Model Bridge

```bash
docker run -d \
  --name model-bridge \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e INPUT_TOPIC=preprocessed \
  -e OUTPUT_TOPIC=predictions \
  -e API_URL=http://model-container:8080 \
  -e API_ENDPOINT=/invocations \
  -e INPUT_FORMAT=json \
  -e OUTPUT_FORMAT=json \
  kafka-bridge:latest
```

### 3. Pipeline complet avec docker-compose

Exemple de `docker-compose.yml` pour le pipeline complet :

```yaml
version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "29092:9092"

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  preprocessing:
    image: preprocessing-remote-lr-0-1-5af69ee1:latest
    environment:
      KAFKA_ENABLED: "true"
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      INPUT_TOPIC: fink-alert
      OUTPUT_TOPIC: preprocessed
      INPUT_FORMAT: avro
      OUTPUT_FORMAT: json
      AVRO_SCHEMA_PATH: /app/schemas/ztf_alert_v3.3.avsc
    depends_on:
      - kafka
    ports:
      - "8000:8000"

  model:
    image: model-remote-lr-0-1-5af69ee1:latest
    ports:
      - "8080:8080"
    depends_on:
      - preprocessing

  model-bridge:
    image: kafka-bridge:latest
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      INPUT_TOPIC: preprocessed
      OUTPUT_TOPIC: predictions
      API_URL: http://model:8080
      API_ENDPOINT: /invocations
      INPUT_FORMAT: json
      OUTPUT_FORMAT: json
      CONSUMER_GROUP_ID: model-bridge-group
    depends_on:
      - kafka
      - model
```

### 4. Vérification du pipeline

```bash
# Vérifier que les containers sont démarrés
docker ps

# Vérifier les logs du preprocessing
docker logs preprocessing

# Vérifier les logs du model bridge
docker logs model-bridge

# Consommer depuis le topic de sortie
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic predictions \
  --from-beginning
```

### Tags MLflow recommandés

Pour que le système récupère automatiquement les informations, ajoutez ces tags dans MLflow :
- `model_name` : Nom du modèle (ex: "model")
- `version` : Version (ex: "v1", "1.0.0")
- `type` : Type de composant ("preprocessing" ou "model")

## API REST (Option 3)

Une API REST est disponible pour déclencher les builds facilement.

### Démarrer l'API

```bash
# Méthode 1 : uvicorn
uvicorn build_scripts.api:app --reload --port 8000

# Méthode 2 : Python direct
python -m build_scripts.api
```

L'API sera disponible sur `http://localhost:8000`

### Documentation interactive

Ouvrez `http://localhost:8000/docs` pour accéder à Swagger UI.

### Endpoints

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/` | Info API |
| GET | `/health` | Health check |
| POST | `/build` | Lancer un build (async) |
| POST | `/build/sync` | Build synchrone (bloquant) |
| GET | `/builds` | Lister les builds |
| GET | `/builds/{id}` | Statut d'un build |
| DELETE | `/builds/{id}` | Supprimer un build |

### Exemples avec curl

```bash
# Health check
curl http://localhost:8000/health

# Lancer un build (asynchrone)
curl -X POST http://localhost:8000/build \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": "e6c1131f4673449aa688ed1ffc3abbbe",
    "preprocessing_path": "code/preprocessing.py"
  }'

# Réponse:
# {"build_id":"a1b2c3d4","status":"pending","message":"Build started..."}

# Vérifier le statut
curl http://localhost:8000/builds/a1b2c3d4

# Build synchrone (attend la fin)
curl -X POST http://localhost:8000/build/sync \
  -H "Content-Type: application/json" \
  -d '{"run_id": "e6c1131f4673449aa688ed1ffc3abbbe"}'

# Lister tous les builds
curl http://localhost:8000/builds

# Filtrer par statut
curl "http://localhost:8000/builds?status=success"
```

### Exemple Python

```python
import requests

# Lancer un build
response = requests.post("http://localhost:8000/build", json={
    "run_id": "e6c1131f4673449aa688ed1ffc3abbbe",
    "preprocessing_path": "code/preprocessing.py",
    "tag": "v1.0.0"
})
build_id = response.json()["build_id"]

# Vérifier le statut
status = requests.get(f"http://localhost:8000/builds/{build_id}").json()
print(f"Status: {status['status']}")
```

---

## CI/CD avec GitHub Actions

Le projet inclut un workflow GitHub Actions pour construire automatiquement les images Docker de preprocessing et de modèle lorsqu'un webhook est déclenché.

### Configuration

1. **Configurer les secrets GitHub** (Settings > Secrets and variables > Actions) :
   - `MLFLOW_TRACKING_URI` : URI du serveur MLflow (ex: `http://mlflow.example.com:5000`)
   - `MLFLOW_USERNAME` : Nom d'utilisateur pour l'authentification MLflow
   - `MLFLOW_PASSWORD` : Mot de passe pour l'authentification MLflow

2. **Format du webhook** :

Le workflow est déclenché via `repository_dispatch` avec le type `mlflow-model-version`. Le payload attendu est :

```json
{
  "ref": "main",
  "entity": "model_version",
  "action": "created",
  "data": {
    "name": "model",
    "version": "2",
    "source": "models:/m-2bd89eedcb14450c8869000d7f4b017c",
    "run_id": "4f8b1b86c344498dba8408e86d14e093",
    "tags": {},
    "description": null
  }
}
```

**Champs importants** :
- `data.run_id` : ID du run MLflow contenant le preprocessing
- `data.name` : Nom du modèle (utilisé pour nommer les images)
- `data.version` : Version du modèle (utilisé pour nommer les images)
- `data.source` : URI MLflow du modèle (format `models:/m-{model_id}`)

### Déclencher le workflow

Pour déclencher le workflow depuis votre système MLflow, envoyez une requête POST à l'API GitHub :

```bash
curl -X POST \
  -H "Accept: application/vnd.github.v3+json" \
  -H "Authorization: token YOUR_GITHUB_TOKEN" \
  https://api.github.com/repos/OWNER/REPO/dispatches \
  -d '{
    "event_type": "mlflow-model-version",
    "client_payload": {
      "ref": "main",
      "entity": "model_version",
      "action": "created",
      "data": {
        "name": "model",
        "version": "2",
        "source": "models:/m-2bd89eedcb14450c8869000d7f4b017c",
        "run_id": "4f8b1b86c344498dba8408e86d14e093"
      }
    }
  }'
```

### Pipeline d'exécution

Le workflow exécute deux jobs séquentiels :

1. **Build Preprocessing** :
   - Télécharge le preprocessing depuis MLflow (via `run_id`)
   - Construit l'image : `preprocessing-{model_name}-{version}:latest`
   - Exemple : `preprocessing-model-2:latest`

2. **Build Model Serve** (uniquement si le preprocessing réussit) :
   - Utilise `mlflow models build-docker` avec `data.source`
   - Construit l'image : `model-{model_name}-{version}:latest`
   - Exemple : `model-model-2:latest`

### Fichiers du workflow

- `.github/workflows/build-mlflow-images.yml` : Workflow GitHub Actions
- `build_scripts/build_model_image.py` : Script pour construire les images MLflow Serve

### Scripts de build

#### Build preprocessing

```bash
python build_scripts/build_image.py {run_id} auto
```

#### Build modèle

```bash
python build_scripts/build_model_image.py {model_source} model-{name}-{version}
```

Exemple :
```bash
python build_scripts/build_model_image.py models:/model-name/1 model-model-name-1
```

---

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
