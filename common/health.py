from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.route("/health")
def health():
    return jsonify({"status": "healthy"})


@health_bp.route("/ready")
def ready():
    return jsonify({"status": "ready"})


@health_bp.route("/metrics")
def metrics_placeholder():
    return jsonify({"message": "Use Prometheus /metrics endpoint"}), 404
