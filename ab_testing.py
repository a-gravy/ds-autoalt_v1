import os, logging
import hashlib
import logging
import csv
from dstools import ab
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


def read_users(target_users_path):
    external_users = set()
    super_users = set()

    with open(target_users_path) as csv_path:
        reader = csv.DictReader(csv_path)
        for line in reader:
            if line["super_user_flg"] == "1":
                super_users.add(line["user_multi_account_id"])
            else:
                external_users.add(line["user_multi_account_id"])

    logging.info(f"nb of external_users = {len(external_users)}")
    logging.info(f"nb of super_users = {len(super_users)}")
    return external_users, super_users


def user_group_split(target_users_path: str, experiment: str, groups: list, super_users_group: str):
    """
    split users into groups and output to files

    output
    * user groups into ${group_name}_users.csv
    * all_available_users
    """
    external_users, super_users = read_users(target_users_path)

    ab_groups = ab.ab_split(list(external_users), experiment, groups)

    ab_groups[super_users_group] += super_users

    # output
    for group_name, users in ab_groups.items():
        logging.info(f"{len(users)} users in group:{group_name}")
        with open(f"{group_name}_users.csv", 'w') as w:
            w.write("user_id\n")
            for user_id in users:
                w.write(f"{user_id}\n")

    with open(f"all_available_users.csv", 'w') as w:
        w.write("user_id\n")
        for user_id in (external_users | super_users):
            w.write(f"{user_id}\n")
