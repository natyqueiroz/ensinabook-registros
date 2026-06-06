from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
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
    conn = psycopg.connect(require_database_url())
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(commit: bool = False):
    with get_db_connection() as conn:
        cursor = conn.cursor(row_factory=dict_row)
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


def normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def rows_to_json(rows):
    return [{k: normalize(v) for k, v in dict(row).items()} for row in rows]


def row_to_json(row):
    if not row:
        return None
    return {k: normalize(v) for k, v in dict(row).items()}


@app.route("/health", methods=["GET"])
def health_check():
    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT 1 AS ok")
        result = cursor.fetchone()
    return jsonify({"status": "ok", "database": result["ok"]})


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json(silent=True) or {}
    password = data.get('password') or request.form.get('password') or ''

    print("JSON:", data)
    print("FORM:", request.form)
    print("PASSWORD RECEBIDA:", repr(password))

    if password.strip() == ACCESS_PASSWORD:
        return jsonify({"message": "Login bem-sucedido"}), 200

    return jsonify({"message": "Senha incorreta"}), 401


# --- Supervisores ---
@app.route("/supervisors", methods=["GET", "POST"])
def handle_supervisors():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "commission"], "Nome e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "INSERT INTO supervisors (name, commission, contact) VALUES (%s, %s, %s) RETURNING id",
                (data.get("name"), data.get("commission"), data.get("contact")),
            )
            new_id = cursor.fetchone()["id"]
        return jsonify({"message": "Supervisor adicionado com sucesso", "id": new_id}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM supervisors ORDER BY id")
        rows = cursor.fetchall()
    return jsonify(rows_to_json(rows))


@app.route("/supervisors/<int:id>", methods=["GET", "POST", "DELETE"])
def handle_supervisor(id):
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "commission"], "Nome e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "UPDATE supervisors SET name = %s, commission = %s, contact = %s WHERE id = %s RETURNING id",
                (data.get("name"), data.get("commission"), data.get("contact"), id),
            )
            if not cursor.fetchone():
                return jsonify({"error": "Supervisor não encontrado"}), 404
        return jsonify({"message": "Supervisor atualizado com sucesso"}), 200

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM supervisors WHERE id = %s RETURNING id", (id,))
            if not cursor.fetchone():
                return jsonify({"error": "Supervisor não encontrado"}), 404
        return jsonify({"message": "Supervisor excluído com sucesso"}), 200

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM supervisors WHERE id = %s", (id,))
        row = cursor.fetchone()
    if row:
        return jsonify(row_to_json(row))
    return jsonify({"error": "Supervisor não encontrado"}), 404


# --- Vendedores ---
@app.route("/sellers", methods=["GET", "POST"])
def handle_sellers():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "supervisor_id", "commission"], "Nome, supervisor e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "INSERT INTO sellers (name, supervisor_id, commission, contact) VALUES (%s, %s, %s, %s) RETURNING id",
                (data.get("name"), data.get("supervisor_id"), data.get("commission"), data.get("contact")),
            )
            new_id = cursor.fetchone()["id"]
        return jsonify({"message": "Vendedor adicionado com sucesso", "id": new_id}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM sellers ORDER BY id")
        rows = cursor.fetchall()
    return jsonify(rows_to_json(rows))


@app.route("/sellers/<int:id>", methods=["GET", "POST", "DELETE"])
def handle_seller(id):
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "supervisor_id", "commission"], "Nome, supervisor e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "UPDATE sellers SET name = %s, supervisor_id = %s, commission = %s, contact = %s WHERE id = %s RETURNING id",
                (data.get("name"), data.get("supervisor_id"), data.get("commission"), data.get("contact"), id),
            )
            if not cursor.fetchone():
                return jsonify({"error": "Vendedor não encontrado"}), 404
        return jsonify({"message": "Vendedor atualizado com sucesso"}), 200

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM sellers WHERE id = %s RETURNING id", (id,))
            if not cursor.fetchone():
                return jsonify({"error": "Vendedor não encontrado"}), 404
        return jsonify({"message": "Vendedor excluído com sucesso"}), 200

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM sellers WHERE id = %s", (id,))
        row = cursor.fetchone()
    if row:
        return jsonify(row_to_json(row))
    return jsonify({"error": "Vendedor não encontrado"}), 404


# --- Clientes ---
@app.route("/clients", methods=["GET", "POST"])
def handle_clients():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name"], "Nome do cliente é obrigatório")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "INSERT INTO clients (name, cpf, email, phone) VALUES (%s, %s, %s, %s) RETURNING id",
                (data.get("name"), data.get("cpf"), data.get("email"), data.get("phone")),
            )
            new_id = cursor.fetchone()["id"]
        return jsonify({"message": "Cliente adicionado com sucesso", "id": new_id}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM clients ORDER BY id")
        rows = cursor.fetchall()
    return jsonify(rows_to_json(rows))


@app.route("/clients/<int:id>", methods=["GET", "POST", "DELETE"])
def handle_client(id):
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name"], "Nome do cliente é obrigatório")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "UPDATE clients SET name = %s, cpf = %s, email = %s, phone = %s WHERE id = %s RETURNING id",
                (data.get("name"), data.get("cpf"), data.get("email"), data.get("phone"), id),
            )
            if not cursor.fetchone():
                return jsonify({"error": "Cliente não encontrado"}), 404
        return jsonify({"message": "Cliente atualizado com sucesso"}), 200

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM clients WHERE id = %s RETURNING id", (id,))
            if not cursor.fetchone():
                return jsonify({"error": "Cliente não encontrado"}), 404
        return jsonify({"message": "Cliente excluído com sucesso"}), 200

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM clients WHERE id = %s", (id,))
        row = cursor.fetchone()
    if row:
        return jsonify(row_to_json(row))
    return jsonify({"error": "Cliente não encontrado"}), 404


# --- Cursos ---
@app.route("/courses", methods=["GET", "POST"])
def handle_courses():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "value"], "Nome e valor do curso são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "INSERT INTO courses (name, value) VALUES (%s, %s) RETURNING id",
                (data.get("name"), data.get("value")),
            )
            new_id = cursor.fetchone()["id"]
        return jsonify({"message": "Curso adicionado com sucesso", "id": new_id}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM courses ORDER BY id")
        rows = cursor.fetchall()
    return jsonify(rows_to_json(rows))


@app.route("/courses/<int:id>", methods=["GET", "POST", "DELETE"])
def handle_course(id):
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "value"], "Nome e valor do curso são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "UPDATE courses SET name = %s, value = %s WHERE id = %s RETURNING id",
                (data.get("name"), data.get("value"), id),
            )
            if not cursor.fetchone():
                return jsonify({"error": "Curso não encontrado"}), 404
        return jsonify({"message": "Curso atualizado com sucesso"}), 200

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM courses WHERE id = %s RETURNING id", (id,))
            if not cursor.fetchone():
                return jsonify({"error": "Curso não encontrado"}), 404
        return jsonify({"message": "Curso excluído com sucesso"}), 200

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT * FROM courses WHERE id = %s", (id,))
        row = cursor.fetchone()
    if row:
        return jsonify(row_to_json(row))
    return jsonify({"error": "Curso não encontrado"}), 404


# --- Vendas ---
SALES_SELECT = """
SELECT s.*, c.name AS client_name, se.name AS seller_name, co.name AS course_name
FROM sales s
JOIN clients c ON s.client_id = c.id
JOIN sellers se ON s.seller_id = se.id
JOIN courses co ON s.course_id = co.id
"""


@app.route("/sales", methods=["GET", "POST"])
def handle_sales():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["contract", "client_id", "seller_id", "course_id", "value", "date"], "Todos os campos são obrigatórios para registrar uma venda")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO sales (contract, client_id, seller_id, course_id, value, gift, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (data.get("contract"), data.get("client_id"), data.get("seller_id"), data.get("course_id"), data.get("value"), data.get("gift", 0.0), data.get("date")),
            )
            new_id = cursor.fetchone()["id"]
        return jsonify({"message": "Venda registrada com sucesso", "id": new_id}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute(SALES_SELECT + " ORDER BY s.id")
        rows = cursor.fetchall()
    return jsonify(rows_to_json(rows))


@app.route("/sales/<int:id>", methods=["GET", "POST", "DELETE"])
def handle_sale(id):
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["contract", "client_id", "seller_id", "course_id", "value", "date"], "Todos os campos são obrigatórios para atualizar uma venda")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE sales
                SET contract = %s, client_id = %s, seller_id = %s, course_id = %s, value = %s, gift = %s, date = %s
                WHERE id = %s RETURNING id
                """,
                (data.get("contract"), data.get("client_id"), data.get("seller_id"), data.get("course_id"), data.get("value"), data.get("gift", 0.0), data.get("date"), id),
            )
            if not cursor.fetchone():
                return jsonify({"error": "Venda não encontrada"}), 404
        return jsonify({"message": "Venda atualizada com sucesso"}), 200

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM sales WHERE id = %s RETURNING id", (id,))
            if not cursor.fetchone():
                return jsonify({"error": "Venda não encontrada"}), 404
        return jsonify({"message": "Venda excluída com sucesso"}), 200

    with get_cursor() as (_conn, cursor):
        cursor.execute(SALES_SELECT + " WHERE s.id = %s", (id,))
        row = cursor.fetchone()
    if row:
        return jsonify(row_to_json(row))
    return jsonify({"error": "Venda não encontrada"}), 404


# --- Cancelamentos ---
CANCELLATIONS_SELECT = """
SELECT ca.*, s.contract, s.value AS sale_value, s.gift AS sale_gift, cl.name AS client_name, se.name AS seller_name
FROM cancellations ca
JOIN sales s ON ca.sale_id = s.id
JOIN clients cl ON s.client_id = cl.id
JOIN sellers se ON s.seller_id = se.id
"""


@app.route("/cancellations", methods=["GET", "POST"])
def handle_cancellations():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["sale_id", "reason", "cancellation_date"], "ID da venda, motivo e data de cancelamento são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "INSERT INTO cancellations (sale_id, reason, cancellation_date, chargeback_date) VALUES (%s, %s, %s, %s) RETURNING id",
                (data.get("sale_id"), data.get("reason"), data.get("cancellation_date"), data.get("chargeback_date")),
            )
            new_id = cursor.fetchone()["id"]
        return jsonify({"message": "Cancelamento registrado com sucesso", "id": new_id}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute(CANCELLATIONS_SELECT + " ORDER BY ca.id")
        rows = cursor.fetchall()
    return jsonify(rows_to_json(rows))


@app.route("/cancellations/<int:id>", methods=["GET", "POST", "DELETE"])
def handle_cancellation(id):
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["sale_id", "reason", "cancellation_date"], "ID da venda, motivo e data de cancelamento são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                "UPDATE cancellations SET sale_id = %s, reason = %s, cancellation_date = %s, chargeback_date = %s WHERE id = %s RETURNING id",
                (data.get("sale_id"), data.get("reason"), data.get("cancellation_date"), data.get("chargeback_date"), id),
            )
            if not cursor.fetchone():
                return jsonify({"error": "Cancelamento não encontrado"}), 404
        return jsonify({"message": "Cancelamento atualizado com sucesso"}), 200

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM cancellations WHERE id = %s RETURNING id", (id,))
            if not cursor.fetchone():
                return jsonify({"error": "Cancelamento não encontrado"}), 404
        return jsonify({"message": "Cancelamento excluído com sucesso"}), 200

    with get_cursor() as (_conn, cursor):
        cursor.execute(CANCELLATIONS_SELECT + " WHERE ca.id = %s", (id,))
        row = cursor.fetchone()
    if row:
        return jsonify(row_to_json(row))
    return jsonify({"error": "Cancelamento não encontrado"}), 404


# --- Relatórios ---
@app.route("/reports", methods=["GET"])
def generate_report():
    report_type = request.args.get("type")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    supervisor_id = request.args.get("supervisor_id")
    seller_id = request.args.get("seller_id")

    with get_cursor() as (_conn, cursor):
        cursor.execute(
            """
            SELECT s.*, se.name AS seller_name, su.name AS supervisor_name,
                   se.commission AS seller_commission, su.commission AS supervisor_commission,
                   se.supervisor_id
            FROM sales s
            JOIN sellers se ON s.seller_id = se.id
            LEFT JOIN supervisors su ON se.supervisor_id = su.id
            """
        )
        all_sales = rows_to_json(cursor.fetchall())

        cursor.execute(
            """
            SELECT ca.*, s.value AS sale_value, s.gift AS sale_gift, s.seller_id
            FROM cancellations ca
            JOIN sales s ON ca.sale_id = s.id
            """
        )
        all_cancellations = rows_to_json(cursor.fetchall())

    filtered_sales = all_sales
    filtered_cancellations = all_cancellations

    if start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        filtered_sales = [s for s in all_sales if start_date <= datetime.strptime(s["date"], "%Y-%m-%d").date() <= end_date]
        filtered_cancellations = [c for c in all_cancellations if start_date <= datetime.strptime(c["cancellation_date"], "%Y-%m-%d").date() <= end_date]

    if supervisor_id:
        seller_ids = {str(s["seller_id"]) for s in filtered_sales if str(s.get("supervisor_id")) == supervisor_id}
        filtered_sales = [s for s in filtered_sales if str(s.get("supervisor_id")) == supervisor_id]
        filtered_cancellations = [c for c in filtered_cancellations if str(c.get("seller_id")) in seller_ids]

    if seller_id:
        filtered_sales = [s for s in filtered_sales if str(s["seller_id"]) == seller_id]
        filtered_cancellations = [c for c in filtered_cancellations if str(c["seller_id"]) == seller_id]

    report_data = []

    if report_type == "seller":
        sellers_map = {}
        for sale in filtered_sales:
            seller_name = sale["seller_name"]
            sellers_map.setdefault(seller_name, {
                "name": seller_name, "type": "Vendedor", "sales_count": 0,
                "cancellations_count": 0, "gross_production": 0.0, "net_production": 0.0,
                "contracts": [], "commission_to_receive": 0.0, "chargeback_to_make": 0.0,
                "net_value": 0.0, "seller_commission_rate": sale["seller_commission"]
            })
            base = float(sale["value"] - sale["gift"])
            sellers_map[seller_name]["sales_count"] += 1
            sellers_map[seller_name]["gross_production"] += float(sale["value"])
            sellers_map[seller_name]["net_production"] += base
            sellers_map[seller_name]["contracts"].append(sale["contract"])
            sellers_map[seller_name]["commission_to_receive"] += base * (float(sale["seller_commission"]) / 100)

        for cancellation in filtered_cancellations:
            original_sale = next((s for s in all_sales if s["id"] == cancellation["sale_id"]), None)
            if original_sale and original_sale["seller_name"] in sellers_map:
                seller_name = original_sale["seller_name"]
                base = float(original_sale["value"] - original_sale["gift"])
                chargeback_amount = base * (float(original_sale["seller_commission"]) / 100)
                sellers_map[seller_name]["cancellations_count"] += 1
                sellers_map[seller_name]["chargeback_to_make"] += chargeback_amount
                sellers_map[seller_name]["commission_to_receive"] -= chargeback_amount

        for item in sellers_map.values():
            item["net_value"] = item["commission_to_receive"]
            report_data.append(item)

    elif report_type == "supervisor":
        supervisors_map = {}
        for sale in filtered_sales:
            supervisor_name = sale.get("supervisor_name") or "Sem supervisor"
            supervisors_map.setdefault(supervisor_name, {
                "name": supervisor_name, "type": "Supervisor", "sales_count": 0,
                "cancellations_count": 0, "gross_production": 0.0, "net_production": 0.0,
                "contracts": [], "commission_to_receive": 0.0, "chargeback_to_make": 0.0,
                "net_value": 0.0, "supervisor_commission_rate": sale.get("supervisor_commission") or 0,
                "seller_commission_rate": sale["seller_commission"]
            })
            base = float(sale["value"] - sale["gift"])
            over_rate = float((sale.get("supervisor_commission") or 0) - sale["seller_commission"])
            supervisors_map[supervisor_name]["sales_count"] += 1
            supervisors_map[supervisor_name]["gross_production"] += float(sale["value"])
            supervisors_map[supervisor_name]["net_production"] += base
            supervisors_map[supervisor_name]["contracts"].append(sale["contract"])
            supervisors_map[supervisor_name]["commission_to_receive"] += base * (over_rate / 100)

        for cancellation in filtered_cancellations:
            original_sale = next((s for s in all_sales if s["id"] == cancellation["sale_id"]), None)
            if original_sale:
                supervisor_name = original_sale.get("supervisor_name") or "Sem supervisor"
                if supervisor_name in supervisors_map:
                    base = float(original_sale["value"] - original_sale["gift"])
                    over_rate = float((original_sale.get("supervisor_commission") or 0) - original_sale["seller_commission"])
                    chargeback_amount = base * (over_rate / 100)
                    supervisors_map[supervisor_name]["cancellations_count"] += 1
                    supervisors_map[supervisor_name]["chargeback_to_make"] += chargeback_amount
                    supervisors_map[supervisor_name]["commission_to_receive"] -= chargeback_amount

        for item in supervisors_map.values():
            item["net_value"] = item["commission_to_receive"]
            report_data.append(item)

    elif report_type in ("daily", "weekly", "monthly"):
        aggregated_data = {}
        for sale in filtered_sales:
            sale_date = datetime.strptime(sale["date"], "%Y-%m-%d").date()
            date_key = sale["date"]
            period_name = sale["date"]

            if report_type == "weekly":
                start_of_week = sale_date - timedelta(days=sale_date.weekday())
                end_of_week = start_of_week + timedelta(days=6)
                date_key = start_of_week.strftime("%Y-%m-%d") + "_week"
                period_name = f"Semana de {start_of_week.strftime('%d/%m')} a {end_of_week.strftime('%d/%m/%Y')}"
            elif report_type == "monthly":
                date_key = sale_date.strftime("%Y-%m")
                period_name = sale_date.strftime("%B de %Y")

            aggregated_data.setdefault(date_key, {
                "name": period_name, "type": report_type.capitalize(), "sales_count": 0,
                "cancellations_count": 0, "gross_production": 0.0, "net_production": 0.0,
                "contracts": [], "commission_to_receive": 0.0, "chargeback_to_make": 0.0,
                "net_value": 0.0
            })
            base = float(sale["value"] - sale["gift"])
            aggregated_data[date_key]["sales_count"] += 1
            aggregated_data[date_key]["gross_production"] += float(sale["value"])
            aggregated_data[date_key]["net_production"] += base
            aggregated_data[date_key]["contracts"].append(sale["contract"])
            aggregated_data[date_key]["commission_to_receive"] += base * (float(sale["seller_commission"]) / 100)

        for cancellation in filtered_cancellations:
            cancellation_date = datetime.strptime(cancellation["cancellation_date"], "%Y-%m-%d").date()
            date_key = cancellation["cancellation_date"]
            if report_type == "weekly":
                start_of_week = cancellation_date - timedelta(days=cancellation_date.weekday())
                date_key = start_of_week.strftime("%Y-%m-%d") + "_week"
            elif report_type == "monthly":
                date_key = cancellation_date.strftime("%Y-%m")

            if date_key in aggregated_data:
                original_sale = next((s for s in all_sales if s["id"] == cancellation["sale_id"]), None)
                if original_sale:
                    base = float(original_sale["value"] - original_sale["gift"])
                    chargeback_amount = base * (float(original_sale["seller_commission"]) / 100)
                    aggregated_data[date_key]["cancellations_count"] += 1
                    aggregated_data[date_key]["chargeback_to_make"] += chargeback_amount
                    aggregated_data[date_key]["commission_to_receive"] -= chargeback_amount

        for data in aggregated_data.values():
            data["net_value"] = data["commission_to_receive"]
            report_data.append(data)
    else:
        raise BadRequestError("Tipo de relatório inválido. Use seller, supervisor, daily, weekly ou monthly.")

    return jsonify(report_data)


# --- Backup e Restauração ---
@app.route("/backup", methods=["GET"])
def backup_data():
    backup = {}
    with get_cursor() as (_conn, cursor):
        for table in ["supervisors", "sellers", "clients", "courses", "sales", "cancellations"]:
            cursor.execute(f"SELECT * FROM {table} ORDER BY id")
            backup[table] = rows_to_json(cursor.fetchall())
    return jsonify(backup)


@app.route("/restore", methods=["POST"])
def restore_data():
    data = parse_json_body()
    with get_cursor(commit=True) as (_conn, cursor):
        cursor.execute("DELETE FROM cancellations")
        cursor.execute("DELETE FROM sales")
        cursor.execute("DELETE FROM courses")
        cursor.execute("DELETE FROM clients")
        cursor.execute("DELETE FROM sellers")
        cursor.execute("DELETE FROM supervisors")

        for s in data.get("supervisors", []):
            cursor.execute(
                "INSERT INTO supervisors (id, name, commission, contact) VALUES (%s, %s, %s, %s)",
                (s["id"], s["name"], s["commission"], s.get("contact")),
            )
        for s in data.get("sellers", []):
            cursor.execute(
                "INSERT INTO sellers (id, name, supervisor_id, commission, contact) VALUES (%s, %s, %s, %s, %s)",
                (s["id"], s["name"], s.get("supervisor_id"), s["commission"], s.get("contact")),
            )
        for c in data.get("clients", []):
            cursor.execute(
                "INSERT INTO clients (id, name, cpf, email, phone) VALUES (%s, %s, %s, %s, %s)",
                (c["id"], c["name"], c.get("cpf"), c.get("email"), c.get("phone")),
            )
        for c in data.get("courses", []):
            cursor.execute(
                "INSERT INTO courses (id, name, value) VALUES (%s, %s, %s)",
                (c["id"], c["name"], c["value"]),
            )
        for s in data.get("sales", []):
            cursor.execute(
                """
                INSERT INTO sales (id, contract, client_id, seller_id, course_id, value, gift, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (s["id"], s["contract"], s["client_id"], s["seller_id"], s["course_id"], s["value"], s.get("gift", 0), s["date"]),
            )
        for c in data.get("cancellations", []):
            cursor.execute(
                """
                INSERT INTO cancellations (id, sale_id, reason, cancellation_date, chargeback_date)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (c["id"], c["sale_id"], c.get("reason"), c["cancellation_date"], c.get("chargeback_date")),
            )

        for table in ["supervisors", "sellers", "clients", "courses", "sales", "cancellations"]:
            cursor.execute(
                "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM " + table + "), 1), true)",
                (table,),
            )

    return jsonify({"message": "Dados restaurados com sucesso"}), 200


@app.route("/")
def serve_index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return send_from_directory(str(STATIC_DIR), "index.html")
    return jsonify({"message": "API Ensinabook online"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
