import docker
import time

client = docker.from_env()

# Shard definitions
shards = [
    {"name": "shard1", "port": 5433},
    {"name": "shard2", "port": 5434},
    {"name": "shard3", "port": 5435},
]

# Database environment
env = {
    "POSTGRES_USER": "pguser",
    "POSTGRES_PASSWORD": "pgpassword",
    "POSTGRES_DB": "appdb"
}

# 1. Creating network
try:
    network = client.networks.create("sharding_network", driver="bridge")
except docker.errors.APIError:
    network = client.networks.get("sharding_network")

# 2. Running shards
for shard in shards:
    try:
        container = client.containers.run(
            "postgres:15",
            name=shard["name"],
            environment=env,
            ports={"5432/tcp": shard["port"]},
            detach=True,
            network="sharding_network",
            volumes={
                f"./{shard['name']}_data": {"bind": "/var/lib/postgresql/data", "mode": "rw"}
            }
        )
        print(f"{shard['name']} started")
    except docker.errors.APIError:
        print(f"{shard['name']} already exists, starting it...")
        container = client.containers.get(shard["name"])
        container.start()

# 3. Building and running router
try:
    router_image = client.images.build(path="./router", tag="sharding-router")[0]
    try:
        router_container = client.containers.run(
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
    except docker.errors.APIError:
        print("Router container already exists, starting it...")
        router_container = client.containers.get("router")
        router_container.start()
except docker.errors.BuildError as e:
    print("Router image build failed:", e)

# 4. Waiting a few seconds for PostgreSQL to initialize
time.sleep(10)
print("All containers should be running now!")
