from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "ensinabook2025")
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "src"


class ConfigError(RuntimeError):
    pass


class BadRequestError(ValueError):
    pass


def require_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ConfigError(
            "DATABASE_URL não configurada. Defina a variável de ambiente DATABASE_URL no Render."
        )
    return database_url


@contextmanager
def get_db_connection():
    conn = psycopg2.connect(require_database_url())
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_cursor(commit: bool = False):
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield conn, cursor
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


@app.errorhandler(ConfigError)
def handle_config_error(exc: ConfigError):
    return jsonify({"error": str(exc)}), 500


@app.errorhandler(BadRequestError)
def handle_bad_request_error(exc: BadRequestError):
    return jsonify({"error": str(exc)}), 400


@app.errorhandler(404)
def handle_not_found(_exc):
    return jsonify({"error": "Rota não encontrada"}), 404


@app.errorhandler(Exception)
def handle_unexpected_error(exc: Exception):
    app.logger.exception("Erro inesperado: %s", exc)
    return jsonify({"error": "Erro interno do servidor"}), 500


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS supervisors (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    commission NUMERIC(10,2) NOT NULL CHECK (commission >= 0),
    contact TEXT
);

CREATE TABLE IF NOT EXISTS sellers (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    supervisor_id BIGINT REFERENCES supervisors (id) ON DELETE SET NULL,
    commission NUMERIC(10,2) NOT NULL CHECK (commission >= 0),
    contact TEXT
);

CREATE TABLE IF NOT EXISTS clients (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    cpf TEXT,
    email TEXT,
    phone TEXT
);

CREATE TABLE IF NOT EXISTS courses (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value NUMERIC(12,2) NOT NULL CHECK (value >= 0)
);

CREATE TABLE IF NOT EXISTS sales (
    id BIGSERIAL PRIMARY KEY,
    contract TEXT NOT NULL UNIQUE,
    client_id BIGINT NOT NULL REFERENCES clients (id) ON DELETE RESTRICT,
    seller_id BIGINT NOT NULL REFERENCES sellers (id) ON DELETE RESTRICT,
    course_id BIGINT NOT NULL REFERENCES courses (id) ON DELETE RESTRICT,
    value NUMERIC(12,2) NOT NULL CHECK (value >= 0),
    gift NUMERIC(12,2) NOT NULL DEFAULT 0.0 CHECK (gift >= 0),
    date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS cancellations (
    id BIGSERIAL PRIMARY KEY,
    sale_id BIGINT NOT NULL UNIQUE REFERENCES sales (id) ON DELETE CASCADE,
    reason TEXT,
    cancellation_date DATE NOT NULL,
    chargeback_date DATE
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date);
CREATE INDEX IF NOT EXISTS idx_sales_seller_id ON sales(seller_id);
CREATE INDEX IF NOT EXISTS idx_sellers_supervisor_id ON sellers(supervisor_id);
CREATE INDEX IF NOT EXISTS idx_cancellations_date ON cancellations(cancellation_date);
"""


def init_db() -> None:
    with get_cursor(commit=True) as (_conn, cursor):
        cursor.execute(SCHEMA_SQL)


@app.before_request
def ensure_db_initialized():
    if not getattr(app, "_db_initialized", False):
        init_db()
        app._db_initialized = True


@app.route("/health", methods=["GET"])
def health_check():
    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT 1 AS ok")
        result = cursor.fetchone()
    return jsonify({"status": "ok", "database": result["ok"]})


@app.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    password = (data.get("password") or request.form.get("password") or "").strip()

    if not password:
        return jsonify({"message": "Senha não enviada"}), 400

    if password == ACCESS_PASSWORD:
        return jsonify({"message": "Login bem-sucedido"}), 200

    return jsonify({"message": "Senha incorreta"}), 401


def parse_json_body() -> dict[str, Any]:
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise BadRequestError("JSON inválido ou ausente")
    return data


def validate_required(data: dict[str, Any], fields: list[str], message: str) -> None:
    for field in fields:
        value = data.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise BadRequestError(message)


# ... mantém todo o resto do seu código igual ...


@app.route("/")
def serve_index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return send_from_directory(str(STATIC_DIR), "index.html")
    return jsonify({"message": "API Ensinabook online"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)