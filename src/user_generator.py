# #!/Users/hardey/Desktop/GITHUB/AnomalyDetectionPipeline/apache-beam/bin/python3

# import json
# import argparse
# from faker import Faker
# from dataclasses import dataclass

# faker = Faker()

# @dataclass
# class User:
#     subscriberId : str
#     srcIP : str

# def generate_user():
#     subscriber_id = faker.uuid4()
#     src_ip = faker.ipv4()
#     return User(subscriber_id, src_ip)
    

# def users_to_json(users, filename):
#     with open(filename, 'w') as file_obj:
#         json.dump(users, file_obj, default=lambda x: x.__dict__)

# def generate_destination_ip(num_dest_ip):
#     return [faker.ipv4_private() for _ in range(num_dest_ip)]
    
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(__file__, description="Netlog Data Generator")
#     parser.add_argument("--num_users", "-e", type=int, dest="num_users", help="The number of users to create", default=10000)
#     parser.add_argument("--output", "-o", type=str, dest="output", help="The output file", default="users.json")
    
#     args = parser.parse_args()
    
#     users = [generate_user() for _ in range(args.num_users)]
#     users_to_json(users, args.output)