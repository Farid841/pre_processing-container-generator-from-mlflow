"""
Runner ultra-simple : charge le preprocessing et appelle pre_processing().
Le preprocessing est déjà dans l'image Docker.
Supporte les formats d'entrée : JSONL (stdin) et fichiers Avro.
"""

import sys
import os
import json
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Le preprocessing est dans /app/preprocessing/
PREPROCESSING_PATH = Path("/app/preprocessing/preprocessing.py")


def load_preprocessing():
    """Charge la fonction pre_processing() depuis le preprocessing."""
    import importlib.util

    if not PREPROCESSING_PATH.exists():
        raise FileNotFoundError(f"Preprocessing not found at {PREPROCESSING_PATH}")

    logger.info(f"Loading preprocessing from {PREPROCESSING_PATH}")

    # Charger le module
    spec = importlib.util.spec_from_file_location("preprocessing", PREPROCESSING_PATH)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module from {PREPROCESSING_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Chercher pre_processing() - Pattern 1: Fonction directe
    if hasattr(module, 'pre_processing') and callable(module.pre_processing):
        func = getattr(module, 'pre_processing')
        if not isinstance(func, type):  # Pas une classe
            logger.info("Found pre_processing() as function")
            return func

    # Chercher pre_processing() - Pattern 2: Classe avec méthode pre_processing()
    for attr_name in dir(module):
        if attr_name.startswith('_'):
            continue

        attr = getattr(module, attr_name)

        # Si c'est une classe avec méthode pre_processing
        if isinstance(attr, type) and hasattr(attr, 'pre_processing'):
            try:
                # Essayer d'instancier (sans arguments si possible)
                instance = attr()
                if callable(getattr(instance, 'pre_processing')):
                    logger.info(f"Found pre_processing() in class {attr_name}")
                    return instance.pre_processing
            except Exception as e:
                logger.warning(f"Could not instantiate {attr_name}: {e}")
                # Essayer comme méthode de classe
                if callable(getattr(attr, 'pre_processing')):
                    logger.info(f"Found pre_processing() as class method in {attr_name}")
                    return getattr(attr, 'pre_processing')

        # Si c'est une instance avec méthode pre_processing
        if hasattr(attr, 'pre_processing') and callable(getattr(attr, 'pre_processing')):
            logger.info(f"Found pre_processing() in instance {attr_name}")
            return attr.pre_processing

    # Pattern 3: Variable pre_processing qui est une fonction
    if 'pre_processing' in dir(module):
        pre_processing_attr = getattr(module, 'pre_processing')
        if callable(pre_processing_attr):
            logger.info("Found pre_processing() as variable")
            return pre_processing_attr

    raise ValueError(
        "No pre_processing() function found in preprocessing. "
        "The code must define a function or class method named 'pre_processing'."
    )


def read_avro_file(file_path):
    """
    Lit un fichier Avro et génère les records un par un.

    Args:
        file_path: Chemin vers le fichier Avro

    Yields:
        dict: Chaque record Avro converti en dictionnaire Python
    """
    try:
        import fastavro
    except ImportError:
        raise ImportError(
            "fastavro not installed. Install with: pip install fastavro"
        )

    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        schema = reader.schema
        logger.info(f"Reading Avro file with schema: {schema.get('name', 'unknown')}")

        for record in reader:
            yield record


def read_avro_from_stdin():
    """
    Lit un fichier Avro depuis stdin (mode binaire).

    Yields:
        dict: Chaque record Avro converti en dictionnaire Python
    """
    try:
        import fastavro
    except ImportError:
        raise ImportError(
            "fastavro not installed. Install with: pip install fastavro"
        )

    # Lire depuis stdin en mode binaire
    reader = fastavro.reader(sys.stdin.buffer)
    schema = reader.schema
    logger.info(f"Reading Avro from stdin with schema: {schema.get('name', 'unknown')}")

    for record in reader:
        yield record


def is_avro_file(file_path_or_stdin):
    """
    Détecte si l'input est un fichier Avro.

    Args:
        file_path_or_stdin: Chemin de fichier ou None pour stdin

    Returns:
        bool: True si c'est un fichier Avro
    """
    if file_path_or_stdin is None:
        # Pour stdin, on essaie de détecter en lisant les premiers bytes
        # Les fichiers Avro commencent par "Obj" + version
        try:
            import sys
            pos = sys.stdin.tell() if hasattr(sys.stdin, 'tell') else 0
            if pos == 0:  # On peut lire depuis le début
                # On va lire depuis stdin.buffer pour voir les bytes
                # Mais on ne peut pas "peek" facilement, donc on assume
                # que si on passe un fichier .avro en argument, c'est Avro
                return False  # Par défaut, on assume JSONL pour stdin
        except:
            return False
    else:
        # Vérifier l'extension
        if str(file_path_or_stdin).endswith('.avro'):
            return True
        # Vérifier le magic number (premiers bytes)
        try:
            with open(file_path_or_stdin, 'rb') as f:
                header = f.read(4)
                # Les fichiers Avro commencent par "Obj" + version (1 byte)
                if header.startswith(b'Obj'):
                    return True
        except Exception:
            pass

    return False


def main():
    """Point d'entrée principal."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run preprocessing on input data (JSONL or Avro)"
    )
    parser.add_argument(
        'input_file',
        nargs='?',
        default=None,
        help='Input file (Avro or JSONL). If not provided, reads from stdin.'
    )
    parser.add_argument(
        '--format',
        choices=['auto', 'jsonl', 'avro'],
        default='auto',
        help='Input format (default: auto-detect)'
    )

    args = parser.parse_args()

    try:
        # Charger pre_processing()
        pre_processing_func = load_preprocessing()
        logger.info("Preprocessing loaded successfully")

        # Déterminer le format d'entrée
        input_format = args.format
        if input_format == 'auto':
            if args.input_file:
                input_format = 'avro' if is_avro_file(args.input_file) else 'jsonl'
            else:
                # Pour stdin, on assume JSONL par défaut
                # L'utilisateur peut forcer avec --format avro
                input_format = 'jsonl'

        logger.info(f"Input format: {input_format}")

        # Lire les données selon le format
        if input_format == 'avro':
            if args.input_file:
                # Lire depuis un fichier Avro
                data_generator = read_avro_file(args.input_file)
            else:
                # Lire depuis stdin (mode binaire)
                data_generator = read_avro_from_stdin()
        else:
            # Format JSONL (ligne par ligne) ou JSON complet
            if args.input_file:
                # Lire depuis un fichier
                def json_generator():
                    with open(args.input_file, 'r') as f:
                        content = f.read().strip()
                        # Essayer de parser comme JSON complet d'abord
                        try:
                            data = json.loads(content)
                            # Si c'est une liste, itérer sur les éléments
                            if isinstance(data, list):
                                for item in data:
                                    yield item
                            else:
                                # Sinon, c'est un seul objet JSON
                                yield data
                        except json.JSONDecodeError:
                            # Sinon, traiter comme JSONL (ligne par ligne)
                            for line in content.split('\n'):
                                if line.strip():
                                    yield json.loads(line)
                data_generator = json_generator()
            else:
                # Lire depuis stdin
                def json_generator():
                    # Essayer de lire tout le contenu d'abord
                    content = sys.stdin.read().strip()
                    if not content:
                        return

                    # Essayer de parser comme JSON complet
                    try:
                        data = json.loads(content)
                        # Si c'est une liste, itérer sur les éléments
                        if isinstance(data, list):
                            for item in data:
                                yield item
                        else:
                            # Sinon, c'est un seul objet JSON
                            yield data
                    except json.JSONDecodeError:
                        # Sinon, traiter comme JSONL (ligne par ligne)
                        for line in content.split('\n'):
                            if line.strip():
                                try:
                                    yield json.loads(line)
                                except json.JSONDecodeError:
                                    logger.warning(f"Ligne JSON invalide ignorée: {line[:50]}...")
                                    continue
                data_generator = json_generator()

        # Traiter chaque record
        for data in data_generator:
            try:
                # Appliquer le preprocessing
                result = pre_processing_func(data)

                # Output (vers Kafka ou stdout)
                # Format JSON ligne par ligne
                print(json.dumps(result, ensure_ascii=False))
                sys.stdout.flush()  # Important pour le streaming

            except Exception as e:
                logger.error(f"Preprocessing failed: {e}", exc_info=True)
                continue

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
