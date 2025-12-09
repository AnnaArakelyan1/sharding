import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import errors
import docker
import time


def wait_for_postgres(host, port, user, password, dbname, timeout=30):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
            conn.close()
            break
        except psycopg2.OperationalError:
            if time.time() - start > timeout:
                raise Exception(f"Postgres at {host}:{port} did not start in {timeout} seconds")
            time.sleep(1)


app = Flask(__name__)

SHARDS = [
    {"host": os.getenv("SHARD1_HOST", "localhost"), "port": int(os.getenv("SHARD1_PORT", 5433))},
    {"host": os.getenv("SHARD2_HOST", "localhost"), "port": int(os.getenv("SHARD2_PORT", 5434))},
    {"host": os.getenv("SHARD3_HOST", "localhost"), "port": int(os.getenv("SHARD3_PORT", 5435))},
]


DB_USER = os.getenv("DB_USER", "pguser")
DB_PASS = os.getenv("DB_PASS", "pgpassword")
DB_NAME = os.getenv("DB_NAME", "appdb")

def get_shard_index(user_id):
    return int(user_id) % len(SHARDS)

def get_conn(shard_idx):
    s = SHARDS[shard_idx]
    return psycopg2.connect(host=s["host"], port=s["port"], user=DB_USER, password=DB_PASS, dbname=DB_NAME)

@app.route("/users", methods=["POST"])
def create_user():
    data = request.json
    if "id" not in data:
        return {"error": "Missing id"}, 400
    shard_idx = get_shard_index(data["id"])
    conn = get_conn(shard_idx)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users (id, name, email) VALUES (%s,%s,%s)",
                    (data["id"], data.get("name"), data.get("email")))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return {"error": str(e)}, 500
    finally:
        cur.close()
        conn.close()
    return {"status": "ok", "shard": shard_idx}, 201

@app.route("/users/<user_id>", methods=["GET"])
def get_user(user_id):
    shard_idx = get_shard_index(user_id)
    conn = get_conn(shard_idx)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return {"error": "not found"}, 404
    return {"user": row, "shard": shard_idx}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)



def add_user(user_id, name, email):
    shard_idx = get_shard_index(user_id)
    conn = get_conn(shard_idx)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("INSERT INTO users (id, name, email) VALUES (%s,%s,%s)",
                    (user_id, name, email))
        conn.commit()
        cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
        row = cur.fetchone()
        return row, shard_idx
    except errors.UniqueViolation:
        conn.rollback()
        # Return None or a message to indicate duplicate
        return None, shard_idx
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


def get_user_by_id(user_id):
    shard_idx = get_shard_index(user_id)
    conn = get_conn(shard_idx)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row, shard_idx



def get_all_users_in_shard(shard_idx):
    """Return all users in a single shard."""
    conn = get_conn(shard_idx)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_all_users():
    """Return all users from all shards as a dict {shard_index: [users]}."""
    all_users = {}
    for i in range(len(SHARDS)):
        all_users[i] = get_all_users_in_shard(i)
    return all_users


def print_all_users():
    """Print all users shard by shard in a readable format."""
    all_users = get_all_users()
    for shard_idx, users in all_users.items():
        print(f"Shard {shard_idx + 1}:")
        if not users:
            print("  No users")
        else:
            for user in users:
                print(f"  ID: {user['id']}, Name: {user['name']}, Email: {user['email']}")
        print("-" * 40)


def delete_user(user_id):
    """Delete a user by ID from its shard."""
    shard_idx = get_shard_index(user_id)
    conn = get_conn(shard_idx)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM users WHERE id=%s", (user_id,))
        conn.commit()
        deleted = cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
    return deleted > 0


def update_user(user_id, name=None, email=None):
    """Update user's name and/or email in the correct shard."""
    shard_idx = get_shard_index(user_id)
    conn = get_conn(shard_idx)
    cur = conn.cursor()
    try:
        # Only update fields provided
        updates = []
        values = []
        if name is not None:
            updates.append("name=%s")
            values.append(name)
        if email is not None:
            updates.append("email=%s")
            values.append(email)
        if not updates:
            return None, shard_idx  # nothing to update
        values.append(user_id)  # for WHERE
        sql = f"UPDATE users SET {', '.join(updates)} WHERE id=%s"
        cur.execute(sql, tuple(values))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
    return get_user_by_id(user_id)  # return updated user and shard



client = docker.from_env()

def add_shard(shard_name=None, port=None):
    """Create a new PostgreSQL shard container and add it to the router."""
    env = {
        "POSTGRES_USER": DB_USER,
        "POSTGRES_PASSWORD": DB_PASS,
        "POSTGRES_DB": DB_NAME
    }

    # Suggest a unique shard name if none provided or if name exists
    existing_names = [s["host"] for s in SHARDS]
    if not shard_name or shard_name in existing_names:
        i = 1
        while f"shard{i}" in existing_names:
            i += 1
        shard_name = f"shard{i}"
        print(f"Using new shard name: {shard_name}")

    # Suggest a unique port if none provided or port exists
    existing_ports = [s["port"] for s in SHARDS]
    if not port or port in existing_ports:
        port = 5432 + len(SHARDS) + 1
        while port in existing_ports:
            port += 1
        print(f"Using new port: {port}")

   
    try:
        container = client.containers.get(shard_name)
        if container.status != "running":
            container.start()
            print(f"{shard_name} started")
        else:
            print(f"{shard_name} already running")
            container_created = False
    except docker.errors.NotFound:
        container = client.containers.run(
            "postgres:15",
            name=shard_name,
            environment=env,
            ports={"5432/tcp": port},
            detach=True,
            network="sharding_network",
            volumes={f"./{shard_name}_data": {"bind": "/var/lib/postgresql/data", "mode": "rw"}}
        )
        print(f"{shard_name} started on port {port}")
        container_created = True

    # Add to SHARDS
    SHARDS.append({"host": "localhost", "port": port})
    print(f"Shard {shard_name} added to router")

    # Wait for postgres only if we **just created the container**
    if container_created:
        wait_for_postgres("localhost", port, DB_USER, DB_PASS, DB_NAME)

    rebalance_shards()



def rebalance_shards():
    """Move users to the correct shard after adding a new shard."""
    all_users = []

    for idx, shard in enumerate(SHARDS):
        conn = get_conn(idx)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255)
);
        """)
        conn.commit()
        cur.close()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM users")
        users = cur.fetchall()
        for u in users:
            u["_shard_index"] = idx
        all_users.extend(users)
        cur.close()
        conn.close()

    for user in all_users:
        new_index = get_shard_index(user["id"])
        old_index = user["_shard_index"]
        if new_index != old_index:
            # Delete from old shard
            old_conn = get_conn(old_index)
            old_cur = old_conn.cursor()
            old_cur.execute("DELETE FROM users WHERE id=%s", (user["id"],))
            old_conn.commit()
            old_cur.close()
            old_conn.close()

            # Insert into new shard
            new_conn = get_conn(new_index)
            new_cur = new_conn.cursor()
            new_cur.execute(
                "INSERT INTO users (id, name, email) VALUES (%s,%s,%s)",
                (user["id"], user["name"], user["email"])
            )
            new_conn.commit()
            new_cur.close()
            new_conn.close()
