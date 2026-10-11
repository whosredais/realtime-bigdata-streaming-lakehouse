import os
import sys
import pytest
from datetime import datetime
import ipaddress

# Ajouter le répertoire parent au sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from log_generator import generate_log
from alert_system import AlertManager

def test_generate_log_structure():
    """Vérifie que la structure du log simulé contient tous les attributs requis."""
    log = generate_log()
    expected_keys = {"ip", "timestamp", "method", "url", "status", "user_agent"}
    assert isinstance(log, dict)
    assert expected_keys.issubset(log.keys())

def test_generate_log_valid_types_and_values():
    """Valide les types de données et les contraintes métier des logs."""
    log = generate_log()
    
    # 1. Validation de l'adresse IP
    ip_obj = ipaddress.ip_address(log["ip"])
    assert ip_obj.version == 4

    # 2. Validation du timestamp ISO
    parsed_date = datetime.fromisoformat(log["timestamp"])
    assert isinstance(parsed_date, datetime)

    # 3. Validation de la méthode HTTP
    assert log["method"] in ["GET", "POST", "PUT", "DELETE"]

    # 4. Validation du code statut
    allowed_statuses = [200, 201, 301, 400, 401, 404, 500, 502, 503]
    assert log["status"] in allowed_statuses

def test_alert_manager_local_logging():
    """Vérifie que l'AlertManager enregistre correctement les alertes en local."""
    manager = AlertManager(min_interval_seconds=0)
    success = manager.send_alert(
        title="Test Unitaire Alerte",
        message="Alerte de test pour la suite CI/CD",
        severity="INFO",
        details={"environment": "pytest"}
    )
    assert success is True
    
    recent = manager.get_recent_alerts(limit=5)
    assert len(recent) > 0
    assert recent[0]["title"] == "Test Unitaire Alerte"
    assert recent[0]["severity"] == "INFO"

def test_alert_manager_rate_limiting():
    """Vérifie le fonctionnement de l'anti-spam de l'AlertManager."""
    manager = AlertManager(min_interval_seconds=60)
    # Première alerte : doit passer
    first = manager.send_alert("Alerte 1", "Premier envoi", "WARNING")
    assert first is True
    
    # Seconde alerte immédiate : doit être bloquée par le rate limit
    second = manager.send_alert("Alerte 2", "Second envoi immédiat", "WARNING")
    assert second is False

def test_airflow_dag_syntax():
    """Vérifie que le fichier DAG Airflow se parse sans erreur de syntaxe."""
    dag_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags", "pipeline_dag.py"))
    assert os.path.exists(dag_path)
    with open(dag_path, "r", encoding="utf-8") as f:
        code = f.read()
    # Compilation Python pour vérifier l'absence d'erreurs de syntaxe
    compiled = compile(code, dag_path, "exec")
    assert compiled is not None
