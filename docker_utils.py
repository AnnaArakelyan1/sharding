import docker
import time

client = docker.from_env()

SHARDS = [
    {"name": "shard1", "port": 5433},
    {"name": "shard2", "port": 5434},
    {"name": "shard3", "port": 5435},
]

DB_ENV = {
    "POSTGRES_USER": "pguser",
    "POSTGRES_PASSWORD": "pgpassword",
    "POSTGRES_DB": "appdb"
}

def create_network():
    try:
        return client.networks.create("sharding_network", driver="bridge")
    except docker.errors.APIError:
        return client.networks.get("sharding_network")

def start_shards():
    network = create_network()
    for shard in SHARDS:
        try:
            client.containers.run(
                "postgres:15",
                name=shard["name"],
                environment=DB_ENV,
                ports={"5432/tcp": shard["port"]},
                detach=True,
                network=network.name,
                volumes={f"./{shard['name']}_data": {"bind": "/var/lib/postgresql/data", "mode": "rw"}}
            )
            print(f"{shard['name']} started")
        except docker.errors.APIError:
            container = client.containers.get(shard["name"])
            container.start()
            print(f"{shard['name']} already exists, starting it...")

def start_router():
    try:
        router_image = client.images.build(path="./router", tag="sharding-router")[0]
        client.containers.run(
            "sharding-router",
            name="router",
            environment={
                "SHARD1_HOST": "shard1",
                "SHARD1_PORT": "5432",
                "SHARD2_HOST": "shard2",
                "SHARD2_PORT": "5432",
                "SHARD3_HOST": "shard3",
                "SHARD3_PORT": "5432",
                "DB_USER": "pguser",
                "DB_PASS": "pgpassword",
                "DB_NAME": "appdb"
            },
            ports={"5000/tcp": 5000},
            detach=True,
            network="sharding_network"
        )
        print("Router started")
    except docker.errors.APIError as e:
        container = client.containers.get("router")
        container.start()
        print("Router container already exists, starting it...")

def start_all():
    start_shards()
    start_router()
    time.sleep(10)  # waiting for Postgres to initialize
    print("All containers should be running now!")
