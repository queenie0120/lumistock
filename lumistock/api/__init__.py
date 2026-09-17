"""Web App 用 REST API（/api/v1）。

目前只有健康檢查；報告、持股、論點卡等端點在 P1 依規格逐步加入。
權限一律在後端檢查；所有回應使用 {data, meta, error} 信封。
"""
from datetime import datetime, timezone

from flask import Blueprint, jsonify

import lumistock


def envelope(data=None, error=None, **meta):
    meta.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
    return {"data": data, "meta": meta, "error": error}


def create_api_blueprint(app_version: str) -> Blueprint:
    bp = Blueprint("lumistock_api_v1", __name__, url_prefix="/api/v1")

    @bp.get("/health")
    def health():
        return jsonify(envelope({"status": "ok", "app_version": app_version,
                                 "package_version": lumistock.__version__}))

    @bp.errorhandler(404)
    def not_found(_e):
        return jsonify(envelope(error={"code": "not_found", "message": "找不到此 API"})), 404

    return bp
