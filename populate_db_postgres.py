from __future__ import annotations

import os

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("Defina DATABASE_URL antes de executar o populate_db_postgres.py")


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cursor.execute(
            """
            TRUNCATE TABLE cancellations, sales, courses, clients, sellers, supervisors
            RESTART IDENTITY CASCADE
            """
        )

        cursor.execute(
            "INSERT INTO supervisors (name, commission, contact) VALUES (%s, %s, %s) RETURNING id",
            ("Kaike", 10.00, "N/A"),
        )
        supervisor_id = cursor.fetchone()["id"]

        cursor.execute(
            "INSERT INTO sellers (name, supervisor_id, commission, contact) VALUES (%s, %s, %s, %s) RETURNING id",
            ("Kaike", supervisor_id, 10.00, "N/A"),
        )
        seller_id = cursor.fetchone()["id"]

        cursor.execute(
            "INSERT INTO clients (name, cpf, email, phone) VALUES (%s, %s, %s, %s) RETURNING id",
            ("Kaike", "123", "kaike@gmail.com", "123"),
        )
        client_id = cursor.fetchone()["id"]

        cursor.execute(
            "INSERT INTO courses (name, value) VALUES (%s, %s) RETURNING id",
            ("Curso 1", 298.90),
        )
        course_id = cursor.fetchone()["id"]

        sales = [
            ("123", client_id, seller_id, course_id, 298.90, 0.00, "2025-08-04"),
            ("124", client_id, seller_id, course_id, 298.90, 0.00, "2025-08-05"),
            ("125", client_id, seller_id, course_id, 298.90, 0.00, "2025-08-10"),
        ]

        cursor.executemany(
            """
            INSERT INTO sales (contract, client_id, seller_id, course_id, value, gift, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            sales,
        )

        conn.commit()
        print("Banco PostgreSQL populado com sucesso.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
