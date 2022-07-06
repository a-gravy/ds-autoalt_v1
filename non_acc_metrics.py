import os, logging
from tqdm import tqdm
import csv
import datetime
from collections import Counter
import numpy as np
import pickle
# from dscollaborative.recommender import ImplicitModel
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


def load_model(path):
    return pickle.load(open(path, "rb"))


class NonAccMetrics:
    def __init__(self, metric_name, black_list_path: str = None):
        self.metric_name = metric_name
        self.all_value = 0
        self.autoalt_value = 0
        self.chotatsu_value = 0
        self.black_sids = set()
        if black_list_path:
            self.load_black_list(black_list_path)

    @staticmethod
    def load_model(model_path):
        # model = ImplicitModel()
        model = load_model(model_path)
        return model

    def load_black_list(self, black_list_path):
        for line in efficient_reading(black_list_path, with_header=True):
            self.black_sids.add(line.rstrip())
        logging.info(f"{len(self.black_sids)} black sids loaded")

    def output(self):
        today = datetime.date.today().isoformat()
        with open('non_acc_metrics.csv', 'w', newline='') as csvfile:
            fieldnames = ['page', 'metric_name', 'all', 'autoalt', 'chotatsu', 'dt']
            # page, metric_name, autoalt, chotatsu, all, ran_on
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            basic_params = {'page': 'MAINPAGE', 'dt': today}
            basic_params.update({'metric_name': self.metric_name, 'all': '{:.2f}'.format(self.all_value), 'autoalt': '{:.2f}'.format(self.autoalt_value), 'chotatsu': '{:.2f}'.format(self.chotatsu_value)})
            writer.writerow(basic_params)


class Novelty(NonAccMetrics):
    def __init__(self, training_data, black_list_path):
        super(Novelty, self).__init__('novelty', black_list_path)
        self.users_set = set()
        self.popularity_cnt = Counter()
        self.load_training_data(training_data)
        self.nb_user = len(self.users_set)

    def load_training_data(self, training_data):
        # user_sakuhin_score.csv
        logging.info("load_training_data")
        with open(training_data, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader):
                self.users_set.add(line['userid'])
                self.popularity_cnt[line['item']] += 1
        logging.info(f'popularity_cnt = {len(self.popularity_cnt)}')
        logging.info(f'users_set = {len(self.users_set)}')

    @staticmethod
    def novelty_calculation(sids, popularity_cnt, nb_user):
        novelty_value = 0.0
        for sid in sids:
            sid_popularity = popularity_cnt.get(sid, 0)
            if sid_popularity:
                novelty_value += -np.log2(sid_popularity/nb_user)
        return novelty_value/len(sids)

    def calculate(self, reco):
        logging.info("novelty_calculation")
        # cal novelty on all SIDs of all ALTs : autoalt & chotatsu
        autoalt_alt_novelty_value = 0.0
        autoalt_alt_cnt = 0

        chotatsu_alt_novelty_value = 0.0
        chotatsu_alt_cnt = 0
        with open(reco, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader):
                if line["user_multi_account_id"] not in ["non-logged-in-coldstart", "coldstart"]:
                    alt_novelty_value = self.novelty_calculation(line["sakuhin_codes"].split("|"), self.popularity_cnt, self.nb_user)

                    if line['autoalt'] == '1':
                        autoalt_alt_novelty_value += alt_novelty_value
                        autoalt_alt_cnt += 1
                    else:
                        chotatsu_alt_novelty_value += alt_novelty_value
                        chotatsu_alt_cnt += 1

        self.all_value = (autoalt_alt_novelty_value+chotatsu_alt_novelty_value)/(autoalt_alt_cnt+chotatsu_alt_cnt)
        self.autoalt_value = autoalt_alt_novelty_value/autoalt_alt_cnt
        self.chotatsu_value = chotatsu_alt_novelty_value/chotatsu_alt_cnt

        logging.info(f"[autotalt] {autoalt_alt_novelty_value}/{autoalt_alt_cnt} = {self.autoalt_value}")
        logging.info(f"[chotatsu] {chotatsu_alt_novelty_value}/{chotatsu_alt_cnt} = {self.chotatsu_value}")
        logging.info(f"[all] {(autoalt_alt_novelty_value+chotatsu_alt_novelty_value)}/{(autoalt_alt_cnt+chotatsu_alt_cnt)} = {self.all_value}")

        self.output()


class CatalogCoverage(NonAccMetrics):
    def __init__(self, model_path, black_list_path=None):
        super(CatalogCoverage, self).__init__('catalog_coverage', black_list_path)
        model = self.load_model(model_path)
        self.nb_all_users = len(model.user_item_matrix.user2id)
        logging.info(f"nb of all users = {self.nb_all_users}")
        # get all items from mode
        self.all_sids = set(model.user_item_matrix.item2id.keys())
        if self.black_sids:
            self.all_sids = self.all_sids - self.black_sids
        logging.info(f"nb of all items = {len(self.all_sids)}")

    def calculate(self, reco_path):
        # reco_sids = set()  # autoalt + chotatsu
        reco_sids_autoalts = set()  #
        reco_sids_chotatsu = set()
        # "/Users/s-chuenkai/PycharmProjects/check/autoalt_ippan_features_2022-07-01.csv"
        with open(reco_path, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader):
                if line["user_multi_account_id"] not in ["non-logged-in-coldstart", "coldstart"]:
                    sids = line["sakuhin_codes"].split("|")
                    # reco_sids.update(sids)
                    if line['autoalt'] == '1':
                        reco_sids_autoalts.update(sids)
                    else:
                        reco_sids_chotatsu.update(sids)

        self.all_value = len((reco_sids_autoalts | reco_sids_chotatsu) & self.all_sids)/len(self.all_sids)
        self.autoalt_value = len(reco_sids_autoalts & self.all_sids)/len(self.all_sids)
        self.chotatsu_value = len(reco_sids_chotatsu & self.all_sids)/len(self.all_sids)

        logging.info(f"[all] nb of items got reco = {len((reco_sids_autoalts | reco_sids_chotatsu) & self.all_sids)}/{len(self.all_sids)} = {self.all_value}")
        logging.info(f"[autoalt] nb of items got reco = {len(reco_sids_autoalts & self.all_sids)}/{len(self.all_sids)}  = {self.autoalt_value}")
        logging.info(f"[chotatsu] nb of items got reco = {len(reco_sids_chotatsu & self.all_sids)}/{len(self.all_sids)}  = {self.chotatsu_value}")

        self.output()

