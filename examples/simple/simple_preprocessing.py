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
        result['processed'] = True
        return result
    return data
