from db_manager import DBManager


def main():
    manager = DBManager(shard_count=4, use_range=False)


    users = [
            (101, "Anna"),
            (202, "David"),
            (303, "Maria"),
            (404, "John"),
            (505, "Emma"),
            (606, "Alex"),
        ]
    
    # print("\n[1] HASH-BASED INSERTS")
    # for uid, name in users:
    #         manager.insert_user(uid, name, method="hash")


    print(manager.get_user(2))

    print("\n GET ALL USERS FROM ALL SHARDS")
    all_users = manager.get_all_users()
    for u in all_users:
        print(u)

    manager.delete_user(404)


    print(manager.get_user(404))

    manager_range = DBManager(shard_count=4, use_range=True)
    
    users_range = [
            (10, "Harry"),
            (250,"Jim")
         
        ]
    
    print("\n[2] RANGE-BASED INSERTS")
    for uid, name in users_range:
            manager_range.insert_user(uid, name)

    print("\n GET ALL USERS FROM ALL SHARDS")
    all_users = manager_range.get_all_users()
    for u in all_users:
        print(u)
    

if __name__ == "__main__":
    main()