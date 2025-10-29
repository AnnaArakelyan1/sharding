import sqlite3
import hashlib
import os

class DBManager:

    def __init__(self, shard_count=4, use_range=False):

        self.shard_count = shard_count
        self.use_range = use_range
        os.makedirs("shards", exist_ok=True)

    def _get_shard_id_hash(self, user_id: int):

        hash_value = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16)
        return hash_value % self.shard_count

    def _get_shard_id_range(self, user_id: int):
  
        if user_id <= 250:
            return 0
        elif user_id <= 500:
            return 1
        elif user_id <= 750:
            return 2
        else:
            return 3

    def _get_connection(self, shard_id: int):
        """Ստանում է sqlite միացումը համապատասխան shard-ի համար"""
        db_path = f"shards/shard_{shard_id}.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        return conn

    def _select_shard(self, user_id: int):
     
        if self.use_range:
            return self._get_shard_id_range(user_id)
        else:
            return self._get_shard_id_hash(user_id)

    def insert_user(self, user_id: int, name: str):
     
        shard_id = self._select_shard(user_id)
        conn = self._get_connection(shard_id)
        conn.execute("INSERT INTO users (id, name) VALUES (?, ?)", (user_id, name))
        conn.commit()
        conn.close()
        print(f"[INFO] User {name} inserted into shard_{shard_id}")

    def get_user(self, user_id: int):
   
        shard_id = self._select_shard(user_id)
        conn = self._get_connection(shard_id)
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        result = cur.fetchone()
        conn.close()
        return result

    def get_all_users(self):
 
        users = []
        for shard_id in range(self.shard_count):
            conn = self._get_connection(shard_id)
            cur = conn.cursor()
            cur.execute("SELECT * FROM users")
            users.extend(cur.fetchall())
            conn.close()
        return users

    def delete_user(self, user_id: int):
 
        shard_id = self._select_shard(user_id)
        conn = self._get_connection(shard_id)
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        conn.close()
        print(f"[INFO] User with id={user_id} deleted from shard_{shard_id}")
        
    def update_user(self, user_id: int, new_name: str):
        shard_id = self._select_shard(user_id)
        conn = self._get_connection(shard_id)
        cur = conn.cursor()
        cur.execute("UPDATE users SET name = ? WHERE id = ?", (new_name, user_id))
        conn.commit()
        conn.close()
        print(f"[INFO] User id={user_id} updated to {new_name} in shard_{shard_id}")

