"""
duplicates_prevention.py

Usage:
    duplicates_prevention.py toppick_to_record --input=PATH
    duplicates_prevention.py check_box --input=PATH

Options:
    -h --help Show this screen
    --version
    --input PATH          File or dir path of input
"""
import os, logging, sys
import plyvel
import yaml
import pickle
from docopt import docopt
PROJECT_PATH = os.path.abspath("%s/.." % os.path.dirname(__file__))
sys.path.append(PROJECT_PATH)
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


class RecoRecord:
    """
    user_id : SID|SID|SID ...
    """
    def __init__(self, record_range=5):
        # self.reco_record_dict = {}
        # Only remove top N SIDs, since user may only see the top N (N = 5)
        self.db = plyvel.DB('recorecord/', create_if_missing=True)
        self.wb = self.db.write_batch()
        self.batch_size = 50000
        self.put_nb = 0
        self.record_range = record_range

    def get_record(self, user_id):
        return self.db.get(user_id.encode('utf-8'), b'').decode('utf-8').split("|")

    def update_record(self, user_id, sids, all=False):
        sids = sids if all else sids[:self.record_range]
        existing_record = self.db.get(user_id.encode('utf-8'), b'').decode('utf-8')
        to_save = existing_record + '|' + '|'.join(sids) if existing_record else '|'.join(sids)
        #self.db.put(user_id.encode('utf-8'), to_save.encode('utf-8'))
        self.wb.put(user_id.encode('utf-8'), to_save.encode('utf-8'))
        self.put_nb += 1
        if (self.put_nb % self.batch_size) == 1:
            logging.info(f"{self.put_nb} data written")
            self.wb.write()

    def toppick_to_record(self, toppick_path):
        for i, line in enumerate(efficient_reading(toppick_path, True, "user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block")):
            arr = line.split(",")
            self.update_record(arr[0], arr[1].split("|")[:self.record_range])

        self.close()

    def show(self):
        for key, value in self.db:
            logging.info(f"{key},{value}")

    def close(self):
        logging.info(f"{self.put_nb} data written")
        self.wb.write()
        self.db.close()

"""
class RecoRecord_:
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
        if user_id in self.reco_record_dict:
            self.reco_record_dict[user_id].update(sids)
            # self.reco_record_dict[user_id] = self.get_record(user_id) | set(sids[:self.record_range])
        else:
            self.reco_record_dict[user_id] = set(sids)

    def toppick_to_record(self, toppick_path):
        for line in efficient_reading(toppick_path, True, "user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block"):
            arr = line.split(",")
            self.reco_record_dict[arr[0]] = set(arr[1].split("|")[:self.record_range])

        self.output_record("recorecord.pkl")
"""


if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    # read dim_autoalt.csv
    if arguments['toppick_to_record']:
        record = RecoRecord()
        record.toppick_to_record(arguments["--input"])
    elif arguments["check_box"]:
        record = RecoRecord()
        record.show()
    else:
        raise Exception("Unimplemented ERROR")


