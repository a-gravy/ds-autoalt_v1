import os
import logging


logging.basicConfig(level=logging.INFO)


def make_demo_candidates(feature_table_path, output_path="demo_candidates.csv"):
    set_dict = {"JFET000001": set(), "JFET000002": set(), "JFET000003": set()}

    with open(feature_table_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                arr = line.split(",")
                feature_public_code = arr[1]

                if set_dict.get(feature_public_code, None) != None:
                    set_dict[feature_public_code].add(arr[0])
            else:
                break

    for k, v in set_dict.items():
        logging.info(f"{k} has {len(v)} users")

    logging.info(f"JFET000001 & JFET000002 & JFET000003 = {len(set_dict['JFET000001'] & set_dict['JFET000002'] & set_dict['JFET000003'])}")

    with open(output_path, "w") as w:
        w.write("user_multi_account_id\n")
        for user_id in (set_dict['JFET000001'] & set_dict['JFET000002'] & set_dict['JFET000003']):
            w.write(f"{user_id}\n")
