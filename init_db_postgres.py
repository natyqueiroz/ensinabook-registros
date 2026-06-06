#!/usr/bin/env python3
"""
Inicializa o banco PostgreSQL da Ensinabook com o mesmo schema usado no main/app.

Uso no Render/Shell:
    export DATABASE_URL="sua_url_postgres"
    python init_database_postgres.py

Opcional:
    SEED_INITIAL_DATA=0 python init_database_postgres.py   # cria tabelas sem dados exemplo
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras


DATABASE_URL = os.getenv("DATABASE_URL")
SEED_INITIAL_DATA = os.getenv("SEED_INITIAL_DATA", "1") != "0"


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


@contextmanager
def get_connection():
    if not DATABASE_URL:
        print("ERRO: variável de ambiente DATABASE_URL não configurada.")
        print("Configure a DATABASE_URL do PostgreSQL no Render antes de executar.")
        sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def init_database() -> None:
    try:
        with get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            print("Criando/validando tabelas compatíveis com o main...")
            cursor.execute(SCHEMA_SQL)
            print("✓ Schema validado com sucesso.")

            if not SEED_INITIAL_DATA:
                print("✓ Inserção de dados iniciais ignorada por SEED_INITIAL_DATA=0.")
                cursor.close()
                return

            cursor.execute("SELECT COUNT(*) AS total FROM supervisors")
            supervisor_count = cursor.fetchone()["total"]

            if supervisor_count > 0:
                print(f"✓ Banco já contém {supervisor_count} supervisor(es). Dados exemplo não inseridos.")
                cursor.close()
                return

            print("Inserindo dados iniciais de exemplo...")

            cursor.execute(
                """
                INSERT INTO supervisors (name, commission, contact)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                ("Kaike", 10.00, "kaike@ensinabook.com"),
            )
            supervisor_id = cursor.fetchone()["id"]

            cursor.execute(
                """
                INSERT INTO sellers (name, supervisor_id, commission, contact)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                ("Kaike", supervisor_id, 30.00, "kaike@ensinabook.com"),
            )
            seller_id = cursor.fetchone()["id"]

            cursor.execute(
                """
                INSERT INTO clients (name, cpf, email, phone)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                ("Kaike", "123.456.789-00", "kaike@email.com", "(11) 98765-4321"),
            )
            client_kaike_id = cursor.fetchone()["id"]

            cursor.execute(
                """
                INSERT INTO clients (name, cpf, email, phone)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                ("Maria Silva", "123.456.789-01", "maria.silva@email.com", "(11) 98765-4321"),
            )
            client_maria_id = cursor.fetchone()["id"]

            cursor.execute(
                """
                INSERT INTO courses (name, value)
                VALUES (%s, %s)
                RETURNING id
                """,
                ("Curso 1", 298.90),
            )
            course_id = cursor.fetchone()["id"]

            sales = [
                ("0001", client_kaike_id, seller_id, course_id, 298.90, 0.00, "2025-08-04"),
                ("0002", client_kaike_id, seller_id, course_id, 298.90, 0.00, "2025-08-05"),
                ("0003", client_maria_id, seller_id, course_id, 298.90, 0.00, "2025-08-10"),
            ]

            cursor.executemany(
                """
                INSERT INTO sales (contract, client_id, seller_id, course_id, value, gift, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (contract) DO NOTHING
                """,
                sales,
            )

            cursor.close()
            print("✓ Dados iniciais inseridos com sucesso.")
            print("✓ Banco de dados inicializado e compatível com o main.")

    except Exception as exc:
        print(f"ERRO ao inicializar banco de dados: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    init_database()
