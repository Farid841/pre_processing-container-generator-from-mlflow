#!/usr/bin/env python3
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN and improved by IA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Setup MLflow local avec preprocessing d'exemple.

Usage:
    python setup_mlflow_local.py
"""

import logging
import sys
from pathlib import Path

# Import pour le modèle
from sklearn.datasets import make_classification
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Vérifier que mlflow est installé
try:
    import mlflow
except ImportError:
    logger.error("❌ MLflow n'est pas installé. Installez avec: pip install mlflow")
    sys.exit(1)

# Configuration MLflow
mlflow.set_tracking_uri("http://127.0.0.1:5000")

# Créer le dossier pour les fichiers preprocessing
preprocessing_dir = Path("preprocessing_mlflow")
preprocessing_dir.mkdir(exist_ok=True)

# Contenu de preprocessing.py
preprocessing_code = '''#!/usr/bin/env python3
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN and improved by IA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Exemple de preprocessing.

Ce code peut être uploadé dans MLflow.
"""


def pre_processing(data):
    """
    Preprocessing des données.

    Args:
        data: Dict avec les données brutes

    Returns:
        Dict avec les données transformées
    """
    if isinstance(data, dict):
        # Nettoyer et normaliser
        result = {k: v for k, v in data.items() if v is not None}
        result["processed"] = True
        return result
    return data
'''

# Contenu de requirements.txt
requirements_content = """# Requirements pour le preprocessing
# Ajoutez vos dépendances ici

# pandas>=1.5.0
# numpy>=1.23.0
# scikit-learn>=1.0.0
"""

# Créer les fichiers
preprocessing_file = preprocessing_dir / "preprocessing.py"
requirements_file = preprocessing_dir / "requirements.txt"

preprocessing_file.write_text(preprocessing_code)
requirements_file.write_text(requirements_content)

logger.info(f"✅ Fichiers créés dans: {preprocessing_dir}")
logger.info("   - preprocessing.py")
logger.info("   - requirements.txt")

# Créer ou récupérer l'experiment
experiment_name = "preprocessing-runs"
try:
    experiment_id = mlflow.create_experiment(experiment_name)
    logger.info(f"✅ Experiment créé: {experiment_name} (ID: {experiment_id})")
except Exception as e:
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment:
        experiment_id = experiment.experiment_id
        logger.info(f"✅ Experiment existant utilisé: {experiment_name} (ID: {experiment_id})")
    else:
        logger.error(f"❌ Erreur lors de la création de l'experiment: {e}")
        sys.exit(1)

# Créer un run avec les tags requis
with mlflow.start_run(experiment_id=experiment_id, run_name="test-preprocessing-v1") as run:
    run_id = run.info.run_id

    # Tags requis pour le build
    mlflow.set_tag("model_name", "test-model")
    mlflow.set_tag("version", "v1.0.0")
    mlflow.set_tag("type", "preprocessing")
    mlflow.set_tag("component_type", "preprocessing")
    mlflow.set_tag("mlflow.runName", "test-preprocessing-v1")

    # Log des paramètres
    mlflow.log_param("preprocessing_type", "simple")
    mlflow.log_param("python_version", sys.version.split()[0])

    # Créer des données d'exemple pour entraîner un modèle
    logger.info("Génération de données d'exemple...")
    X, y = make_classification(
        n_samples=1000, n_features=20, n_informative=15, n_redundant=5, n_classes=2, random_state=42
    )
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Paramètres du modèle
    learning_rate = 0.1
    params = {"learning_rate": learning_rate, "random_state": 42, "max_iter": 100}

    # Entraîner le modèle
    logger.info("Entraînement du modèle HistGradientBoostingClassifier...")
    model = HistGradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # Prédictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Calculer les métriques réelles
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_test, y_pred_proba)

    # Logger les paramètres du modèle
    mlflow.log_params(params)

    # Logger les métriques réelles
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("roc_auc", roc_auc)

    # Logger le modèle scikit-learn
    logger.info("Log du modèle dans MLflow...")
    from mlflow.models import infer_signature

    signature = infer_signature(X_train, y_pred)
    mlflow.sklearn.log_model(
        model, artifact_path="model", signature=signature, input_example=X_train[:5]
    )

    # Uploader les fichiers preprocessing (à la racine du run, pas de sous-dossier)
    mlflow.log_artifact(str(preprocessing_file))
    mlflow.log_artifact(str(requirements_file))

    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ Run MLflow créé avec succès!")
    logger.info("")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Experiment: {experiment_name}")
    logger.info("")
    logger.info("Tags créés:")
    logger.info("  - model_name: test-model")
    logger.info("  - version: v1.0.0")
    logger.info("  - type: preprocessing")
    logger.info("")
    logger.info("Paramètres loggés:")
    logger.info("  - preprocessing_type: simple")
    logger.info("  - python_version: " + sys.version.split()[0])
    logger.info("")
    logger.info("Métriques calculées et loggées:")
    logger.info(f"  - accuracy: {accuracy:.4f}")
    logger.info(f"  - precision: {precision:.4f}")
    logger.info(f"  - recall: {recall:.4f}")
    logger.info(f"  - f1_score: {f1:.4f}")
    logger.info(f"  - roc_auc: {roc_auc:.4f}")
    logger.info("")
    logger.info("Modèle loggé:")
    logger.info("  - HistGradientBoostingClassifier dans artifact_path='model'")
    logger.info("")
    logger.info("Artifacts uploadés:")
    logger.info("  - preprocessing.py")
    logger.info("  - requirements.txt")
    logger.info("")
    logger.info("Vous pouvez maintenant tester le build avec:")
    logger.info(f"  ./build.sh {run_id}")
    logger.info("")
    logger.info("Ou voir dans MLflow UI: http://127.0.0.1:5000")
    logger.info("=" * 60)
