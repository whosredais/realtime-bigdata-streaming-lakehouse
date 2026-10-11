import os
import json
import time
import requests
from datetime import datetime

class AlertManager:
    """
    Système d'Alerting et de Notifcations en Temps Réel pour le Pipeline Big Data.
    Prend en charge les Webhooks Discord / Slack avec mode Fallback (console & fichier de logs).
    """
    def __init__(self, webhook_url=None, min_interval_seconds=30):
        self.webhook_url = webhook_url or os.getenv("ALERT_WEBHOOK_URL")
        self.min_interval = min_interval_seconds
        self.last_alert_time = 0
        self.log_dir = os.path.join(os.path.dirname(__file__), "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self.alert_history_file = os.path.join(self.log_dir, "alerts_history.json")

    def send_alert(self, title, message, severity="WARNING", details=None):
        """
        Envoie une alerte formatée avec gestion anti-spam (rate-limiting).
        Severity: INFO, WARNING, CRITICAL
        """
        now = time.time()
        if now - self.last_alert_time < self.min_interval:
            print(f"[AlertManager] Alerte mise en attente (rate limit actif): {title}")
            return False

        self.last_alert_time = now
        timestamp = datetime.utcnow().isoformat()
        alert_payload = {
            "title": title,
            "message": message,
            "severity": severity,
            "timestamp": timestamp,
            "details": details or {}
        }

        # 1. Enregistrement dans l'historique local
        self._record_locally(alert_payload)

        # 2. Envoi via Webhook (Discord / Slack) si configuré
        if self.webhook_url:
            try:
                # Format compatible Discord & Slack
                color_map = {
                    "INFO": 3447003,      # Bleu
                    "WARNING": 16776960,  # Jaune
                    "CRITICAL": 15158332  # Rouge
                }
                discord_payload = {
                    "username": "BigData Pipeline AlertBot",
                    "embeds": [{
                        "title": f"[{severity}] {title}",
                        "description": message,
                        "color": color_map.get(severity.upper(), 16776960),
                        "timestamp": timestamp,
                        "fields": [
                            {"name": k, "value": str(v), "inline": True} 
                            for k, v in (details or {}).items()
                        ]
                    }]
                }
                resp = requests.post(self.webhook_url, json=discord_payload, timeout=5)
                if resp.status_code in [200, 204]:
                    print(f"[AlertManager] Alerte transmise avec succès au Webhook: {title}")
                    return True
                else:
                    print(f"[AlertManager] Échec envoi Webhook (Code {resp.status_code})")
            except Exception as e:
                print(f"[AlertManager] Erreur réseau Webhook : {e}")

        # 3. Fallback d'affichage console riche
        emoji = "🔴" if severity == "CRITICAL" else ("🟡" if severity == "WARNING" else "ℹ️")
        print(f"\n{'='*60}\n{emoji} [ALERTE PIPELINE - {severity}] {title}\n{message}\nDétails: {details}\n{'='*60}\n")
        return True

    def _record_locally(self, payload):
        """Stocke l'alerte dans un fichier JSON pour consultation par le Dashboard."""
        try:
            history = []
            if os.path.exists(self.alert_history_file):
                with open(self.alert_history_file, "r", encoding="utf-8") as f:
                    history = json.load(f)
            history.append(payload)
            # Conserver les 100 dernières alertes
            history = history[-100:]
            with open(self.alert_history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            print(f"[AlertManager] Erreur sauvegarde locale alerte : {e}")

    def get_recent_alerts(self, limit=20):
        """Récupère les alertes récentes pour le Dashboard Streamlit."""
        try:
            if os.path.exists(self.alert_history_file):
                with open(self.alert_history_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return list(reversed(data))[:limit]
        except Exception:
            pass
        return []

if __name__ == "__main__":
    manager = AlertManager()
    manager.send_alert(
        title="Pic critique d'erreurs HTTP 500 détecté",
        message="Le volume d'erreurs 500 sur les 10 dernières secondes a dépassé le seuil de tolérance (> 5 erreurs).",
        severity="CRITICAL",
        details={"Window": "10s", "Errors_500": 7, "Action": "Investigation requise sur les microservices"}
    )
