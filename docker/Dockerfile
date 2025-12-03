# Image de base Python
FROM python:3.10-slim

WORKDIR /app

# Installer les dépendances système (si besoin)
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python de base (runner)
RUN pip install --no-cache-dir fastavro>=1.4.0

# Copier le runner
COPY runner/ /app/runner/

# Copier le script d'entrée
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Créer le dossier pour le preprocessing
RUN mkdir -p /app/preprocessing

# Le preprocessing sera copié ici au build-time
# (via build_scripts/build_image.py qui ajoute ces lignes dynamiquement)
# COPY preprocessing/ /app/preprocessing/
# RUN pip install --no-cache-dir -r /app/preprocessing/requirements.txt

# Point d'entrée
ENTRYPOINT ["/app/entrypoint.sh"]
