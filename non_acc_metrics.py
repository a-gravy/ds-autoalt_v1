import os, logging
from tqdm import tqdm
import csv
import datetime
from collections import Counter
import numpy as np
import sklearn.metrics.pairwise as pairwise
from dscollaborative.recommender import ImplicitModel
from scipy.sparse import lil_matrix
from numpy import dot
from numpy.linalg import norm
import math
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


class NonAccMetrics:
    def __init__(self, metric_name, black_list_path: str = None):
        self.miniters = 500000  # for tqdm, update progress per miniters
        self.metric_name = metric_name
        self.all_value = 0
        self.autoalt_value = 0
        self.chotatsu_value = 0
        self.black_sids = set()
        if black_list_path:
            self.load_black_list(black_list_path)

    @staticmethod
    def load_model(model_path):
        model = ImplicitModel()
        model.load_model(model_path)
        return model

    @staticmethod
    def cos_similarity(a, b):
        if any(a) and any(b):
            return dot(a, b)/(norm(a)*norm(b))
        else:  # if one of input is all zeros -> one of input has no features -> skip
            return np.nan

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


class SimilarityMetric(NonAccMetrics):
    def __init__(self, metric_name, model_path, sakuhin_meta_path: str):
        super(SimilarityMetric, self).__init__(metric_name)
        self.feat_idx_lookup = self.make_item_feat_vectors(sakuhin_meta_path)
        # hard-coded for v1
        self.feat_idx_table = {'JFET000002':0, 'JFET000003':1, 'JFET000004':2, 'JFET000005':3,
                               'JFET0000061':4, 'JFET0000062':4, 'JFET0000063':4, 'JFET0000064':4, 'JFET0000065':4, 'JFET0000066':4, 'JFET0000067':4,
                               'JFET000007':5, 'JFET000008':6, 'chotatsu':7}
        self.feat_names = ['byw', 'new arrival EPs', 'popular', 'trending', 'tag', 'exclusives', 'new_arrival SIDs', 'chotatsu']
        model = self.load_model(model_path)
        self.item2id = model.model.user_item_matrix.item2id
        self.user2id = model.model.user_item_matrix.user2id
        del model

        self.item_feat_matrix = np.zeros((len(self.item2id), len(self.feat_idx_lookup)))
        genre_weight = 2
        with open(sakuhin_meta_path, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader, miniters=self.miniters):
                item_idx = self.item2id.get(line['sakuhin_public_code'], None)
                if not item_idx:
                    continue

                genre_idx = self.feat_idx_lookup.get(line['main_genre_code'], None)
                if not genre_idx:
                    continue
                self.item_feat_matrix[item_idx, genre_idx] = genre_weight

                tag_pool = set()
                tag_pool.update(line['menu_names'].split('|'))
                tag_pool.update(line['parent_menu_names'].split('|'))
                for tag in tag_pool:
                    tag_idx = self.feat_idx_lookup.get(tag, None)
                    self.item_feat_matrix[item_idx, tag_idx] = 1
                # nation_pool.update(line['nations'].split('/'))
        logging.info(f"item_feat_matrix ({self.item_feat_matrix.shape}) built")

    @staticmethod
    def pairwise_matrix_cos_sim(vectors):
        """
        calculate cos_sim between items inside a list,
        :param vectors: array of vectors
        :return:
        """
        s = pairwise.cosine_similarity(vectors)
        div = (2*(s.shape[0]*s.shape[0]-s.shape[0])/2)
        if div <= 0.00001:
            return 0
        else:
            return (np.sum(s) - s.shape[0])/div

    @staticmethod
    def cos_sim_2d_intra_list(vectors):
        """
        calculate cos_sim between items inside a list,
        faster than pairwise_matrix_cos_sim
        :param vectors: array of vectors
        :return:
        """
        norm_x = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
        s = np.matmul(norm_x, norm_x.T)
        div = (2*(s.shape[0]*s.shape[0]-s.shape[0])/2)
        return (np.sum(s) - s.shape[0])/div

    @staticmethod
    def make_item_feat_vectors(sakuhin_meta_path: str) -> dict:
        genre_pool = set()  # main_genre_code
        tag_pool = set()  # menu_names
        #nation_pool = set()
        with open(sakuhin_meta_path, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in reader:
                genre_pool.add(line['main_genre_code'])
                tag_pool.update(line['menu_names'].split('|'))
                tag_pool.update(line['parent_menu_names'].split('|'))

        feat_idx_lookup = dict()
        for i, x in enumerate((genre_pool | tag_pool) - {''}) :
            feat_idx_lookup[x] = i
        return feat_idx_lookup


class Serendipity(SimilarityMetric):
    def __init__(self, model_path: str, sakuhin_meta_path: str, user_sid_history_path: str):
        super(Serendipity, self).__init__('serendipity', model_path, sakuhin_meta_path)
        self.top_n = 5  # only run on top N items

        # build history record matrix
        logging.info("building history record matrix")
        self.history_matrix = lil_matrix((len(self.user2id), len(self.item2id)), dtype=int)
        # history SIDs may be too heavy
        # user_multi_account_id,sids,genres,play_times
        with open(user_sid_history_path, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader, miniters=self.miniters):
                user_idx = self.user2id.get(line['user_multi_account_id'], None)
                sids = line["sids"].split("|")[:self.top_n]
                sid_indices = [self.item2id[x] for x in sids if x in self.item2id]
                if not user_idx or not sid_indices:
                    continue
                self.history_matrix[user_idx, sid_indices] = 1

    def cal_unserendipity(self, history, reco):
        overall_sim_list = []
        for h_i in history:
            for r_i in reco:
                cos_sim = self.cos_similarity(self.item_feat_matrix[h_i], self.item_feat_matrix[r_i])
                if not math.isnan(cos_sim):
                    overall_sim_list.append(cos_sim)

        return np.mean(overall_sim_list)

    @staticmethod
    def cos_sim_2d(x, y):
        norm_x = x / np.linalg.norm(x, axis=1, keepdims=True)
        norm_y = y / np.linalg.norm(y, axis=1, keepdims=True)
        return np.mean(np.matmul(norm_x, norm_y.T))

    def calculate(self, reco):
        logging.info("start calculating ...")
        metric_matrix = np.zeros((len(self.user2id), self.feat_idx_table['chotatsu']+1))

        with open(reco, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader, miniters=self.miniters):
                if line["user_multi_account_id"] not in ["non-logged-in-coldstart", "coldstart"]:
                    user_idx = self.user2id.get(line['user_multi_account_id'], None)
                    sids = line["sakuhin_codes"].split("|")[:self.top_n]
                    r_sid_indices = [self.item2id[x] for x in sids if x in self.item2id]
                    if not user_idx or not r_sid_indices:
                        continue
                    h_sid_indices = self.history_matrix.rows[user_idx]
                    if not h_sid_indices:
                        continue

                    # serendipity_value = self.cal_unserendipity(h_sid_indices, r_sid_indices)
                    cos_sim = self.cos_sim_2d(self.item_feat_matrix[h_sid_indices], self.item_feat_matrix[r_sid_indices])
                    if not math.isnan(cos_sim):
                        alt_idx = self.feat_idx_table.get(line['feature_public_code'], self.feat_idx_table['chotatsu'])
                        metric_matrix[user_idx, alt_idx] = cos_sim

        alt_sim_sums = np.sum(metric_matrix, axis=0)
        alt_sim_cnts = np.count_nonzero(metric_matrix, axis=0)
        for name, alt_sim_sum, alt_sim_cnt in zip(self.feat_names, alt_sim_sums, alt_sim_cnts):
            logging.info("{} = {:.2f}%".format(name, (1.0 - (alt_sim_sum/alt_sim_cnt))*100))

        self.all_value = 1 - np.sum(alt_sim_sums)/np.sum(alt_sim_cnts)
        self.autoalt_value = 1 - np.sum(alt_sim_sums)[:-1]/np.sum(alt_sim_cnts)[:-1]
        self.chotatsu_value = 1 - np.sum(alt_sim_sums)[-1]/np.sum(alt_sim_cnts)[-1]
        self.output()


class Diversity(SimilarityMetric):
    def __init__(self, model_path: str, sakuhin_meta_path: str):
        super(Diversity, self).__init__('diversity', model_path, sakuhin_meta_path)

    def calculate(self, reco):
        metric_matrix = np.zeros((len(self.user2id), self.feat_idx_table['chotatsu']+1))
        with open(reco, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader, miniters=self.miniters):
                if line["user_multi_account_id"] not in ["non-logged-in-coldstart", "coldstart"]:
                    sids = line["sakuhin_codes"].split("|")
                    user_idx = self.user2id.get(line['user_multi_account_id'], None)
                    sid_indices = [self.item2id[x] for x in sids if x in self.item2id]
                    if not user_idx or not sid_indices:
                        continue
                    alt_idx = self.feat_idx_table.get(line['feature_public_code'], self.feat_idx_table['chotatsu'])
                    similarity = self.cos_sim_2d_intra_list(self.item_feat_matrix[sid_indices])
                    if similarity:
                        metric_matrix[user_idx, alt_idx] = similarity

        alt_sim_sum = np.sum(metric_matrix, axis=0)
        alt_sim_cnt = np.count_nonzero(metric_matrix, axis=0)
        for name, v in zip(self.feat_names, alt_sim_sum/alt_sim_cnt):
            logging.info("{} = {:.2f}%".format(name, (1-v)*100))

        self.all_value = 1 - np.sum(alt_sim_sum)/np.sum(alt_sim_cnt)
        self.autoalt_value = 1 - np.sum(alt_sim_sum)[:-1]/np.sum(alt_sim_cnt)[:-1]
        self.chotatsu_value = 1 - np.sum(alt_sim_sum)[-1]/np.sum(alt_sim_cnt)[-1]
        self.output()


class Novelty(NonAccMetrics):
    def __init__(self, training_data, black_list_path):
        super(Novelty, self).__init__('novelty', black_list_path)
        self.users_set = set()
        self.popularity_cnt = Counter()
        self.load_training_data(training_data)
        self.nb_user = len(self.users_set)

    def load_training_data(self, training_data):
        logging.info("load_training_data")
        with open(training_data, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader, miniters=self.miniters):
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
            for line in tqdm(reader, miniters=self.miniters):
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
        self.nb_all_users = len(model.model.user_item_matrix.user2id)
        logging.info(f"nb of all users = {self.nb_all_users}")
        # get all items from mode
        self.all_sids = set(model.model.user_item_matrix.item2id.keys())
        if self.black_sids:
            self.all_sids = self.all_sids - self.black_sids
        logging.info(f"nb of all items = {len(self.all_sids)}")

    def calculate(self, reco_path):
        reco_sids_autoalts = set()
        reco_sids_chotatsu = set()
        with open(reco_path, "r") as csv_path:
            reader = csv.DictReader(csv_path)
            for line in tqdm(reader, miniters=self.miniters):
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
