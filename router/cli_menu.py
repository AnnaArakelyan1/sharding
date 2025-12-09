from app import add_user, get_user_by_id, delete_user, get_all_users_in_shard, get_all_users,update_user,SHARDS,add_shard

def menu():
    while True:
        print("\n=== Shard User Management ===")
        print("1. Add user")
        print("2. Get user by ID")
        print("3. Delete user")
        print("4. List all users in a shard")
        print("5. List all users in all shards")
        print("6. Update user")
        print("7. Add new shard and rebalance users")
        print("0. Exit")
        choice = input("Select an option: ")

        if choice == "1":
            uid = int(input("User ID: "))
            
            # Checking if user already exists
            existing_user, shard = get_user_by_id(uid)
            if existing_user:
                print(f"Error: User ID {uid} already exists on shard {shard}.")
                continue  

            name = input("Name: ")
            email = input("Email: ")
            user, shard = add_user(uid, name, email)
            if user:
                print(f"User {user['name']} added to shard {shard}")

        elif choice == "2":
            uid = int(input("User ID: "))
            user, shard = get_user_by_id(uid)
            print(f"User: {user} in shard {shard}")

        elif choice == "3":
            uid = int(input("User ID: "))
            delete_user(uid)
            print(f"Deleted user with ID {uid}")

        elif choice == "4":
            shard_idx = int(input("Shard index: "))
            users = get_all_users_in_shard(shard_idx)
            print(f"Users in shard {shard_idx}:")
            for u in users:
                print(u)

        elif choice == "5":
            all_users_dict = get_all_users()
            for shard_idx, users in all_users_dict.items():
                print(f"\nShard {shard_idx}:")
                for u in users:
                    print(u)

        elif choice == "6":
            uid = int(input("User ID to update: "))
            name = input("New name (leave blank to skip): ")
            email = input("New email (leave blank to skip): ")
            try:
                user, shard = update_user(uid, name if name else None, email if email else None)
                print(f"Updated user: {user} in shard {shard}")
            except Exception as e:
                print("Error:", e)
        elif choice == "7":
            shard_name = input("Enter new shard name: ").strip()           

            if any(s["host"] == shard_name for s in SHARDS):
                print(f"Error: Shard name '{shard_name}' already exists.")
                continue
            port = int(input("Enter port for new shard: "))
            if any(s["port"] == port for s in SHARDS):
                print(f"Error: Port {port} is already in use by another shard.")
                continue

            add_shard(shard_name, port)

        elif choice == "0":
            print("Exiting...")
            break
        else:
            print("Invalid option. Try again.")

if __name__ == "__main__":
    menu()
