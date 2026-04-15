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


@app.route("/supervisors", methods=["GET", "POST"])
def handle_supervisors():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "commission"], "Nome e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO supervisors (name, commission, contact)
                VALUES (%s, %s, %s)
                RETURNING id, name, commission, contact
                """,
                (data["name"], data["commission"], data.get("contact")),
            )
            supervisor = cursor.fetchone()
        return jsonify({"message": "Supervisor adicionado com sucesso", "data": supervisor}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, commission, contact FROM supervisors ORDER BY id")
        supervisors = cursor.fetchall()
    return jsonify(supervisors)


@app.route("/supervisors/<int:item_id>", methods=["GET", "PUT", "DELETE"])
def handle_supervisor(item_id: int):
    if request.method == "PUT":
        data = parse_json_body()
        validate_required(data, ["name", "commission"], "Nome e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE supervisors
                SET name = %s, commission = %s, contact = %s
                WHERE id = %s
                RETURNING id, name, commission, contact
                """,
                (data["name"], data["commission"], data.get("contact"), item_id),
            )
            supervisor = cursor.fetchone()
        if not supervisor:
            return jsonify({"error": "Supervisor não encontrado"}), 404
        return jsonify({"message": "Supervisor atualizado com sucesso", "data": supervisor})

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM supervisors WHERE id = %s RETURNING id", (item_id,))
            deleted = cursor.fetchone()
        if not deleted:
            return jsonify({"error": "Supervisor não encontrado"}), 404
        return jsonify({"message": "Supervisor excluído com sucesso"})

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, commission, contact FROM supervisors WHERE id = %s", (item_id,))
        supervisor = cursor.fetchone()
    if not supervisor:
        return jsonify({"error": "Supervisor não encontrado"}), 404
    return jsonify(supervisor)


@app.route("/sellers", methods=["GET", "POST"])
def handle_sellers():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "supervisor_id", "commission"], "Nome, supervisor e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO sellers (name, supervisor_id, commission, contact)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, supervisor_id, commission, contact
                """,
                (data["name"], data["supervisor_id"], data["commission"], data.get("contact")),
            )
            seller = cursor.fetchone()
        return jsonify({"message": "Vendedor adicionado com sucesso", "data": seller}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, supervisor_id, commission, contact FROM sellers ORDER BY id")
        sellers = cursor.fetchall()
    return jsonify(sellers)


@app.route("/sellers/<int:item_id>", methods=["GET", "PUT", "DELETE"])
def handle_seller(item_id: int):
    if request.method == "PUT":
        data = parse_json_body()
        validate_required(data, ["name", "supervisor_id", "commission"], "Nome, supervisor e comissão são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE sellers
                SET name = %s, supervisor_id = %s, commission = %s, contact = %s
                WHERE id = %s
                RETURNING id, name, supervisor_id, commission, contact
                """,
                (data["name"], data["supervisor_id"], data["commission"], data.get("contact"), item_id),
            )
            seller = cursor.fetchone()
        if not seller:
            return jsonify({"error": "Vendedor não encontrado"}), 404
        return jsonify({"message": "Vendedor atualizado com sucesso", "data": seller})

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM sellers WHERE id = %s RETURNING id", (item_id,))
            deleted = cursor.fetchone()
        if not deleted:
            return jsonify({"error": "Vendedor não encontrado"}), 404
        return jsonify({"message": "Vendedor excluído com sucesso"})

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, supervisor_id, commission, contact FROM sellers WHERE id = %s", (item_id,))
        seller = cursor.fetchone()
    if not seller:
        return jsonify({"error": "Vendedor não encontrado"}), 404
    return jsonify(seller)


@app.route("/clients", methods=["GET", "POST"])
def handle_clients():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name"], "Nome do cliente é obrigatório")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO clients (name, cpf, email, phone)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, cpf, email, phone
                """,
                (data["name"], data.get("cpf"), data.get("email"), data.get("phone")),
            )
            client = cursor.fetchone()
        return jsonify({"message": "Cliente adicionado com sucesso", "data": client}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, cpf, email, phone FROM clients ORDER BY id")
        clients = cursor.fetchall()
    return jsonify(clients)


@app.route("/clients/<int:item_id>", methods=["GET", "PUT", "DELETE"])
def handle_client(item_id: int):
    if request.method == "PUT":
        data = parse_json_body()
        validate_required(data, ["name"], "Nome do cliente é obrigatório")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE clients
                SET name = %s, cpf = %s, email = %s, phone = %s
                WHERE id = %s
                RETURNING id, name, cpf, email, phone
                """,
                (data["name"], data.get("cpf"), data.get("email"), data.get("phone"), item_id),
            )
            client = cursor.fetchone()
        if not client:
            return jsonify({"error": "Cliente não encontrado"}), 404
        return jsonify({"message": "Cliente atualizado com sucesso", "data": client})

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM clients WHERE id = %s RETURNING id", (item_id,))
            deleted = cursor.fetchone()
        if not deleted:
            return jsonify({"error": "Cliente não encontrado"}), 404
        return jsonify({"message": "Cliente excluído com sucesso"})

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, cpf, email, phone FROM clients WHERE id = %s", (item_id,))
        client = cursor.fetchone()
    if not client:
        return jsonify({"error": "Cliente não encontrado"}), 404
    return jsonify(client)


@app.route("/courses", methods=["GET", "POST"])
def handle_courses():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(data, ["name", "value"], "Nome e valor do curso são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO courses (name, value)
                VALUES (%s, %s)
                RETURNING id, name, value
                """,
                (data["name"], data["value"]),
            )
            course = cursor.fetchone()
        return jsonify({"message": "Curso adicionado com sucesso", "data": course}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, value FROM courses ORDER BY id")
        courses = cursor.fetchall()
    return jsonify(courses)


@app.route("/courses/<int:item_id>", methods=["GET", "PUT", "DELETE"])
def handle_course(item_id: int):
    if request.method == "PUT":
        data = parse_json_body()
        validate_required(data, ["name", "value"], "Nome e valor do curso são obrigatórios")
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE courses
                SET name = %s, value = %s
                WHERE id = %s
                RETURNING id, name, value
                """,
                (data["name"], data["value"], item_id),
            )
            course = cursor.fetchone()
        if not course:
            return jsonify({"error": "Curso não encontrado"}), 404
        return jsonify({"message": "Curso atualizado com sucesso", "data": course})

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM courses WHERE id = %s RETURNING id", (item_id,))
            deleted = cursor.fetchone()
        if not deleted:
            return jsonify({"error": "Curso não encontrado"}), 404
        return jsonify({"message": "Curso excluído com sucesso"})

    with get_cursor() as (_conn, cursor):
        cursor.execute("SELECT id, name, value FROM courses WHERE id = %s", (item_id,))
        course = cursor.fetchone()
    if not course:
        return jsonify({"error": "Curso não encontrado"}), 404
    return jsonify(course)


@app.route("/sales", methods=["GET", "POST"])
def handle_sales():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(
            data,
            ["contract", "client_id", "seller_id", "course_id", "value", "date"],
            "Todos os campos são obrigatórios para registrar uma venda",
        )
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO sales (contract, client_id, seller_id, course_id, value, gift, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, contract, client_id, seller_id, course_id, value, gift, date
                """,
                (
                    data["contract"],
                    data["client_id"],
                    data["seller_id"],
                    data["course_id"],
                    data["value"],
                    data.get("gift", 0.0),
                    data["date"],
                ),
            )
            sale = cursor.fetchone()
        return jsonify({"message": "Venda registrada com sucesso", "data": sale}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute(
            """
            SELECT s.id, s.contract, s.client_id, s.seller_id, s.course_id, s.value, s.gift,
                   TO_CHAR(s.date, 'YYYY-MM-DD') AS date,
                   c.name AS client_name,
                   se.name AS seller_name,
                   co.name AS course_name
            FROM sales s
            JOIN clients c ON s.client_id = c.id
            JOIN sellers se ON s.seller_id = se.id
            JOIN courses co ON s.course_id = co.id
            ORDER BY s.id
            """
        )
        sales = cursor.fetchall()
    return jsonify(sales)


@app.route("/sales/<int:item_id>", methods=["GET", "PUT", "DELETE"])
def handle_sale(item_id: int):
    if request.method == "PUT":
        data = parse_json_body()
        validate_required(
            data,
            ["contract", "client_id", "seller_id", "course_id", "value", "date"],
            "Todos os campos são obrigatórios para atualizar uma venda",
        )
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE sales
                SET contract = %s,
                    client_id = %s,
                    seller_id = %s,
                    course_id = %s,
                    value = %s,
                    gift = %s,
                    date = %s
                WHERE id = %s
                RETURNING id, contract, client_id, seller_id, course_id, value, gift, date
                """,
                (
                    data["contract"],
                    data["client_id"],
                    data["seller_id"],
                    data["course_id"],
                    data["value"],
                    data.get("gift", 0.0),
                    data["date"],
                    item_id,
                ),
            )
            sale = cursor.fetchone()
        if not sale:
            return jsonify({"error": "Venda não encontrada"}), 404
        return jsonify({"message": "Venda atualizada com sucesso", "data": sale})

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM sales WHERE id = %s RETURNING id", (item_id,))
            deleted = cursor.fetchone()
        if not deleted:
            return jsonify({"error": "Venda não encontrada"}), 404
        return jsonify({"message": "Venda excluída com sucesso"})

    with get_cursor() as (_conn, cursor):
        cursor.execute(
            """
            SELECT s.id, s.contract, s.client_id, s.seller_id, s.course_id, s.value, s.gift,
                   TO_CHAR(s.date, 'YYYY-MM-DD') AS date,
                   c.name AS client_name,
                   se.name AS seller_name,
                   co.name AS course_name
            FROM sales s
            JOIN clients c ON s.client_id = c.id
            JOIN sellers se ON s.seller_id = se.id
            JOIN courses co ON s.course_id = co.id
            WHERE s.id = %s
            """,
            (item_id,),
        )
        sale = cursor.fetchone()
    if not sale:
        return jsonify({"error": "Venda não encontrada"}), 404
    return jsonify(sale)


@app.route("/cancellations", methods=["GET", "POST"])
def handle_cancellations():
    if request.method == "POST":
        data = parse_json_body()
        validate_required(
            data,
            ["sale_id", "reason", "cancellation_date"],
            "ID da venda, motivo e data de cancelamento são obrigatórios",
        )
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                INSERT INTO cancellations (sale_id, reason, cancellation_date, chargeback_date)
                VALUES (%s, %s, %s, %s)
                RETURNING id, sale_id, reason,
                          TO_CHAR(cancellation_date, 'YYYY-MM-DD') AS cancellation_date,
                          CASE WHEN chargeback_date IS NULL THEN NULL ELSE TO_CHAR(chargeback_date, 'YYYY-MM-DD') END AS chargeback_date
                """,
                (data["sale_id"], data["reason"], data["cancellation_date"], data.get("chargeback_date")),
            )
            cancellation = cursor.fetchone()
        return jsonify({"message": "Cancelamento registrado com sucesso", "data": cancellation}), 201

    with get_cursor() as (_conn, cursor):
        cursor.execute(
            """
            SELECT ca.id, ca.sale_id, ca.reason,
                   TO_CHAR(ca.cancellation_date, 'YYYY-MM-DD') AS cancellation_date,
                   CASE WHEN ca.chargeback_date IS NULL THEN NULL ELSE TO_CHAR(ca.chargeback_date, 'YYYY-MM-DD') END AS chargeback_date,
                   s.contract,
                   s.value AS sale_value,
                   s.gift AS sale_gift,
                   cl.name AS client_name,
                   se.name AS seller_name
            FROM cancellations ca
            JOIN sales s ON ca.sale_id = s.id
            JOIN clients cl ON s.client_id = cl.id
            JOIN sellers se ON s.seller_id = se.id
            ORDER BY ca.id
            """
        )
        cancellations = cursor.fetchall()
    return jsonify(cancellations)


@app.route("/cancellations/<int:item_id>", methods=["GET", "PUT", "DELETE"])
def handle_cancellation(item_id: int):
    if request.method == "PUT":
        data = parse_json_body()
        validate_required(
            data,
            ["sale_id", "reason", "cancellation_date"],
            "ID da venda, motivo e data de cancelamento são obrigatórios",
        )
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute(
                """
                UPDATE cancellations
                SET sale_id = %s,
                    reason = %s,
                    cancellation_date = %s,
                    chargeback_date = %s
                WHERE id = %s
                RETURNING id, sale_id, reason,
                          TO_CHAR(cancellation_date, 'YYYY-MM-DD') AS cancellation_date,
                          CASE WHEN chargeback_date IS NULL THEN NULL ELSE TO_CHAR(chargeback_date, 'YYYY-MM-DD') END AS chargeback_date
                """,
                (
                    data["sale_id"],
                    data["reason"],
                    data["cancellation_date"],
                    data.get("chargeback_date"),
                    item_id,
                ),
            )
            cancellation = cursor.fetchone()
        if not cancellation:
            return jsonify({"error": "Cancelamento não encontrado"}), 404
        return jsonify({"message": "Cancelamento atualizado com sucesso", "data": cancellation})

    if request.method == "DELETE":
        with get_cursor(commit=True) as (_conn, cursor):
            cursor.execute("DELETE FROM cancellations WHERE id = %s RETURNING id", (item_id,))
            deleted = cursor.fetchone()
        if not deleted:
            return jsonify({"error": "Cancelamento não encontrado"}), 404
        return jsonify({"message": "Cancelamento excluído com sucesso"})

    with get_cursor() as (_conn, cursor):
        cursor.execute(
            """
            SELECT ca.id, ca.sale_id, ca.reason,
                   TO_CHAR(ca.cancellation_date, 'YYYY-MM-DD') AS cancellation_date,
                   CASE WHEN ca.chargeback_date IS NULL THEN NULL ELSE TO_CHAR(ca.chargeback_date, 'YYYY-MM-DD') END AS chargeback_date,
                   s.contract,
                   s.value AS sale_value,
                   s.gift AS sale_gift,
                   cl.name AS client_name,
                   se.name AS seller_name
            FROM cancellations ca
            JOIN sales s ON ca.sale_id = s.id
            JOIN clients cl ON s.client_id = cl.id
            JOIN sellers se ON s.seller_id = se.id
            WHERE ca.id = %s
            """,
            (item_id,),
        )
        cancellation = cursor.fetchone()
    if not cancellation:
        return jsonify({"error": "Cancelamento não encontrado"}), 404
    return jsonify(cancellation)


@app.route("/reports", methods=["GET"])
def generate_report():
    report_type = request.args.get("type", "seller")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    supervisor_id = request.args.get("supervisor_id")
    seller_id = request.args.get("seller_id")

    with get_cursor() as (_conn, cursor):
        cursor.execute(
            """
            SELECT s.id,
                   s.contract,
                   s.value,
                   s.gift,
                   TO_CHAR(s.date, 'YYYY-MM-DD') AS date,
                   s.seller_id,
                   se.name AS seller_name,
                   se.commission AS seller_commission,
                   se.supervisor_id,
                   su.name AS supervisor_name,
                   COALESCE(su.commission, 0) AS supervisor_commission
            FROM sales s
            JOIN sellers se ON s.seller_id = se.id
            LEFT JOIN supervisors su ON se.supervisor_id = su.id
            ORDER BY s.id
            """
        )
        all_sales = cursor.fetchall()

        cursor.execute(
            """
            SELECT ca.id,
                   ca.sale_id,
                   ca.reason,
                   TO_CHAR(ca.cancellation_date, 'YYYY-MM-DD') AS cancellation_date,
                   CASE WHEN ca.chargeback_date IS NULL THEN NULL ELSE TO_CHAR(ca.chargeback_date, 'YYYY-MM-DD') END AS chargeback_date,
                   s.seller_id
            FROM cancellations ca
            JOIN sales s ON ca.sale_id = s.id
            ORDER BY ca.id
            """
        )
        all_cancellations = cursor.fetchall()

    def within_range(date_text: str) -> bool:
        if not start_date_str or not end_date_str:
            return True
        dt = datetime.strptime(date_text, "%Y-%m-%d").date()
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        return start_date <= dt <= end_date

    filtered_sales = [sale for sale in all_sales if within_range(sale["date"])]
    filtered_cancellations = [
        cancellation for cancellation in all_cancellations if within_range(cancellation["cancellation_date"])
    ]

    if supervisor_id:
        filtered_sales = [sale for sale in filtered_sales if str(sale["supervisor_id"]) == str(supervisor_id)]
        sale_ids = {sale["id"] for sale in filtered_sales}
        filtered_cancellations = [c for c in filtered_cancellations if c["sale_id"] in sale_ids]

    if seller_id:
        filtered_sales = [sale for sale in filtered_sales if str(sale["seller_id"]) == str(seller_id)]
        sale_ids = {sale["id"] for sale in filtered_sales}
        filtered_cancellations = [c for c in filtered_cancellations if c["sale_id"] in sale_ids]

    sales_by_id = {sale["id"]: sale for sale in filtered_sales}

    def calculate_amount(value: Any, gift: Any, rate: Any) -> float:
        return float((float(value) - float(gift)) * (float(rate) / 100.0))

    if report_type == "seller":
        report_map: dict[str, dict[str, Any]] = {}
        for sale in filtered_sales:
            key = sale["seller_name"]
            report_map.setdefault(
                key,
                {
                    "name": sale["seller_name"],
                    "type": "Vendedor",
                    "sales_count": 0,
                    "cancellations_count": 0,
                    "gross_production": 0.0,
                    "net_production": 0.0,
                    "contracts": [],
                    "gross_commission": 0.0,
                    "chargeback_to_make": 0.0,
                    "net_value": 0.0,
                    "commission_rate": float(sale["seller_commission"]),
                },
            )
            item = report_map[key]
            item["sales_count"] += 1
            item["gross_production"] += float(sale["value"])
            item["net_production"] += float(sale["value"]) - float(sale["gift"])
            item["contracts"].append(sale["contract"])
            item["gross_commission"] += calculate_amount(sale["value"], sale["gift"], sale["seller_commission"])

        for cancellation in filtered_cancellations:
            sale = sales_by_id.get(cancellation["sale_id"])
            if not sale:
                continue
            item = report_map[sale["seller_name"]]
            item["cancellations_count"] += 1
            item["chargeback_to_make"] += calculate_amount(sale["value"], sale["gift"], sale["seller_commission"])

        result = list(report_map.values())
        for item in result:
            item["net_value"] = item["gross_commission"] - item["chargeback_to_make"]
            item["commission_to_receive"] = item["net_value"]
        return jsonify(result)

    if report_type == "supervisor":
        report_map = {}
        for sale in filtered_sales:
            key = sale["supervisor_name"] or "Sem supervisor"
            report_map.setdefault(
                key,
                {
                    "name": key,
                    "type": "Supervisor",
                    "sales_count": 0,
                    "cancellations_count": 0,
                    "gross_production": 0.0,
                    "net_production": 0.0,
                    "contracts": [],
                    "gross_commission": 0.0,
                    "chargeback_to_make": 0.0,
                    "net_value": 0.0,
                    "commission_rate": float(sale["supervisor_commission"]),
                },
            )
            item = report_map[key]
            item["sales_count"] += 1
            item["gross_production"] += float(sale["value"])
            item["net_production"] += float(sale["value"]) - float(sale["gift"])
            item["contracts"].append(sale["contract"])
            item["gross_commission"] += calculate_amount(sale["value"], sale["gift"], sale["supervisor_commission"])

        for cancellation in filtered_cancellations:
            sale = sales_by_id.get(cancellation["sale_id"])
            if not sale:
                continue
            key = sale["supervisor_name"] or "Sem supervisor"
            item = report_map[key]
            item["cancellations_count"] += 1
            item["chargeback_to_make"] += calculate_amount(sale["value"], sale["gift"], sale["supervisor_commission"])

        result = list(report_map.values())
        for item in result:
            item["net_value"] = item["gross_commission"] - item["chargeback_to_make"]
            item["commission_to_receive"] = item["net_value"]
        return jsonify(result)

    if report_type in {"daily", "weekly", "monthly"}:
        report_map = {}
        for sale in filtered_sales:
            sale_date = datetime.strptime(sale["date"], "%Y-%m-%d").date()
            if report_type == "daily":
                key = sale_date.strftime("%Y-%m-%d")
                label = sale_date.strftime("%d/%m/%Y")
            elif report_type == "weekly":
                start_of_week = sale_date - timedelta(days=sale_date.weekday())
                end_of_week = start_of_week + timedelta(days=6)
                key = start_of_week.strftime("%Y-%m-%d")
                label = f"Semana de {start_of_week.strftime('%d/%m/%Y')} a {end_of_week.strftime('%d/%m/%Y')}"
            else:
                key = sale_date.strftime("%Y-%m")
                label = sale_date.strftime("%m/%Y")

            report_map.setdefault(
                key,
                {
                    "name": label,
                    "type": report_type.capitalize(),
                    "sales_count": 0,
                    "cancellations_count": 0,
                    "gross_production": 0.0,
                    "net_production": 0.0,
                    "contracts": [],
                    "gross_commission": 0.0,
                    "chargeback_to_make": 0.0,
                    "net_value": 0.0,
                },
            )
            item = report_map[key]
            item["sales_count"] += 1
            item["gross_production"] += float(sale["value"])
            item["net_production"] += float(sale["value"]) - float(sale["gift"])
            item["contracts"].append(sale["contract"])
            item["gross_commission"] += calculate_amount(sale["value"], sale["gift"], sale["seller_commission"])

        for cancellation in filtered_cancellations:
            sale = sales_by_id.get(cancellation["sale_id"])
            if not sale:
                continue
            cancellation_date = datetime.strptime(cancellation["cancellation_date"], "%Y-%m-%d").date()
            if report_type == "daily":
                key = cancellation_date.strftime("%Y-%m-%d")
            elif report_type == "weekly":
                key = (cancellation_date - timedelta(days=cancellation_date.weekday())).strftime("%Y-%m-%d")
            else:
                key = cancellation_date.strftime("%Y-%m")
            if key not in report_map:
                continue
            item = report_map[key]
            item["cancellations_count"] += 1
            item["chargeback_to_make"] += calculate_amount(sale["value"], sale["gift"], sale["seller_commission"])

        result = list(report_map.values())
        for item in result:
            item["net_value"] = item["gross_commission"] - item["chargeback_to_make"]
            item["commission_to_receive"] = item["net_value"]
        return jsonify(result)

    return jsonify({"error": "Tipo de relatório inválido"}), 400


@app.route("/backup", methods=["GET"])
def backup_data():
    with get_cursor() as (_conn, cursor):
        backup: dict[str, list[dict[str, Any]]] = {}
        tables = ["supervisors", "sellers", "clients", "courses", "sales", "cancellations"]
        for table in tables:
            cursor.execute(f"SELECT * FROM {table} ORDER BY id")
            rows = cursor.fetchall()
            for row in rows:
                for key, value in list(row.items()):
                    if hasattr(value, "isoformat"):
                        row[key] = value.isoformat()
            backup[table] = rows
    return jsonify(backup)


@app.route("/restore", methods=["POST"])
def restore_data():
    data = parse_json_body()
    with get_cursor(commit=True) as (_conn, cursor):
        cursor.execute("TRUNCATE TABLE cancellations, sales, courses, clients, sellers, supervisors RESTART IDENTITY CASCADE")

        for row in data.get("supervisors", []):
            cursor.execute(
                "INSERT INTO supervisors (id, name, commission, contact) VALUES (%s, %s, %s, %s)",
                (row["id"], row["name"], row["commission"], row.get("contact")),
            )
        for row in data.get("sellers", []):
            cursor.execute(
                "INSERT INTO sellers (id, name, supervisor_id, commission, contact) VALUES (%s, %s, %s, %s, %s)",
                (row["id"], row["name"], row.get("supervisor_id"), row["commission"], row.get("contact")),
            )
        for row in data.get("clients", []):
            cursor.execute(
                "INSERT INTO clients (id, name, cpf, email, phone) VALUES (%s, %s, %s, %s, %s)",
                (row["id"], row["name"], row.get("cpf"), row.get("email"), row.get("phone")),
            )
        for row in data.get("courses", []):
            cursor.execute(
                "INSERT INTO courses (id, name, value) VALUES (%s, %s, %s)",
                (row["id"], row["name"], row["value"]),
            )
        for row in data.get("sales", []):
            cursor.execute(
                """
                INSERT INTO sales (id, contract, client_id, seller_id, course_id, value, gift, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["id"],
                    row["contract"],
                    row["client_id"],
                    row["seller_id"],
                    row["course_id"],
                    row["value"],
                    row.get("gift", 0.0),
                    row["date"],
                ),
            )
        for row in data.get("cancellations", []):
            cursor.execute(
                """
                INSERT INTO cancellations (id, sale_id, reason, cancellation_date, chargeback_date)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    row["id"],
                    row["sale_id"],
                    row.get("reason"),
                    row["cancellation_date"],
                    row.get("chargeback_date"),
                ),
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