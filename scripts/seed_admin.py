"""Seed the first admin user with has_full_access = true.

Usage:
    python scripts/seed_admin.py admin@example.com --password "S3cureP@ss!" "Admin Name"
    python scripts/seed_admin.py admin@example.com --google  # Google SSO admin (no password)
    python scripts/seed_admin.py admin@example.com  # prompts for password interactively
"""

import getpass
import sys
import uuid

import bcrypt

from app.db import connection as db_conn
from app.db import prediction_db


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/seed_admin.py <email> [--password <pw>] [--google] [name]")
        sys.exit(1)

    args = sys.argv[1:]
    email = args.pop(0)

    password = None
    auth_method = "local"
    name = "Admin"

    while args:
        arg = args.pop(0)
        if arg == "--password" and args:
            password = args.pop(0)
        elif arg == "--google":
            auth_method = "google"
        else:
            name = arg

    db_conn.connect()
    prediction_db.connect()

    existing = prediction_db.get_user_by_email(email)
    if existing:
        print(f"User {email} already exists (id={existing['id']})")
        conn = prediction_db.get_conn()
        conn.execute("UPDATE users SET has_full_access = true WHERE id = %s", [existing["id"]])
        print(f"Set has_full_access = true for {email}")
        return

    if auth_method == "local" and password is None:
        password = getpass.getpass(f"Password for {email}: ")
        if len(password) < 8:
            print("Error: password must be at least 8 characters")
            sys.exit(1)

    password_hash = _hash_password(password) if password else None

    user_id = str(uuid.uuid4())
    prediction_db.create_user(user_id, email, name, auth_method=auth_method, password_hash=password_hash)
    prediction_db.verify_user(user_id)
    conn = prediction_db.get_conn()
    conn.execute("UPDATE users SET has_full_access = true WHERE id = %s", [user_id])
    print(f"Created admin user: {email} (id={user_id}, auth_method={auth_method}, has_full_access=true)")


if __name__ == "__main__":
    main()
