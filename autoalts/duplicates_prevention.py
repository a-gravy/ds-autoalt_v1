"""
duplicates_prevention.py

Usage:
    duplicates_prevention.py toppick_to_record --input=PATH

Options:
    -h --help Show this screen
    --version
    --input PATH          File or dir path of input
"""
import os, logging
import yaml
import pickle
from docopt import docopt
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


class RecoRecord:
    def __init__(self, record_range=5):
        self.reco_record_dict = {}
        # Only remove top N SIDs, since user may only see the top N (N = 5)
        self.record_range = record_range

    def read_record(self, record_path="data/recorecord.pkl"):
        logging.info(f"reading {record_path}")
        with open(record_path, "rb") as fp:
            self.reco_record_dict = pickle.load(fp)
        logging.info(f"[RecoRecord] read {len(self.reco_record_dict)} users' record ")

    def output_record(self, record_path="recorecord.pkl"):
        with open(record_path, "wb") as fp:
            pickle.dump(self.reco_record_dict, fp)
        logging.info(f"output RecoRecord to {record_path}")

    def get_record(self, user_id):
        return self.reco_record_dict.get(user_id, set())

    def update_record(self, user_id, sids):
        self.reco_record_dict[user_id] = self.get_record(user_id) | set(sids[:self.record_range])

    def toppick_to_record(self, toppick_path):
        for line in efficient_reading(toppick_path, True, "user_multi_account_id,platform,block,sakuhin_codes,feature_name,sub_text,score,create_date"):
            arr = line.split(",")
            self.reco_record_dict[arr[0]] = set(arr[3].split("|")[:self.record_range])

        self.output_record("recorecord.pkl")


if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    # read dim_autoalt.csv
    if arguments['toppick_to_record']:
        record = RecoRecord()
        record.toppick_to_record(arguments["--input"])
    else:
        raise Exception("Unimplemented ERROR")


