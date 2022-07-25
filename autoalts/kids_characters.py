"""
kids_characters

Usage:
    kids_characters.py data_processing --input=PATH
    kids_characters.py make_alt --input=PATH --model=PATH  <top_N>
    kids_characters.py make_coldstart --input=PATH


Options:
    -h --help Show this screen
    --version
    --input PATH          File or dir path of input
    --output PATH         File or dir path of output
    --model PATH          File path location of the trained model
"""
import os, sys, tempfile, logging, time
from dscollaborative.recommender import UserItemRatingMatrix, ImplicitModel, Features, Cleaner, Duplicates
import numpy as np
import pandas as pd
import pickle
import datetime
from collections import defaultdict
import operator
from docopt import docopt
from tqdm import tqdm
PROJECT_PATH = os.path.abspath("%s/.." % os.path.dirname(__file__))
sys.path.append(PROJECT_PATH)
from autoalts.utils import batch, efficient_reading


logging.basicConfig(level=logging.INFO)


def data_processing(data_path):
    """
    data source: 新作業用 in https://docs.google.com/spreadsheets/d/1fVLXMetXMMBAOTX4tFKSBoEU7zJk33k4TfBkP27WubY/edit#gid=1204238941

    process the file from business side

    :return: a character mapping
    """

    # TODO: how to sync up with the google sheet

    # remove the first line
    if not os.path.exists("tmp.csv"):  # TODO tmp for dev
        with open("tmp.csv", "w") as w:
            with open(data_path, "r") as r:
                r.readline()
                for line in r.readlines():
                    w.write(line)

    # read as dataframe and show some info
    df = pd.read_csv("tmp.csv", usecols=['作品公開コード', '作品名', 'キャラクター名', 'シート内キャラID', 'キャラレベル'])
    for col in ['作品公開コード', '作品名', 'キャラクター名', 'シート内キャラID', 'キャラレベル']:
        logging.info(f"nb of data in {col} is {df[col].notnull().sum()}")

    # remove '除外' character
    pdf = df.dropna(axis=0, how='any', subset=['キャラレベル'], inplace=False)
    pdf = pdf[pdf['キャラレベル'] != '除外']

    gdf = pdf.groupby('キャラクター名',as_index=False).aggregate(lambda pdf: pdf.unique().tolist())

    # 'HUNTER×HUNTER': ['SID0002812', 'SID0002956' ...]
    with open("kids_character_name_sakuhins.pkl", "wb") as fp:
        pickle.dump({k:v for k, v in zip(gdf['キャラクター名'], gdf['作品公開コード'])}, fp)

    # 'KC0031': ['SID0002812', 'SID0002956', ...]
    with open("kids_character_id_sakuhins.pkl", "wb") as fp:   #Pickling
        pickle.dump({k[0]:v for k, v in zip(gdf['シート内キャラID'], gdf['作品公開コード'])}, fp)

    logging.info("output processed data to data/kids_character_name_sakuhins.pkl & data/kids_character_id_sakuhins.pkl")


class Ranker:
    def __init__(self, model_path):
        self.collab_model = ImplicitModel()
        self.collab_model.load_model(model_path)

    def get_users_in_model(self, target_users):
        filtered_users = self.collab_model.filter_users(target_users)
        logging.info(f"{len(target_users)} users -> {len(filtered_users)} users after filtering")
        return filtered_users

    def fet_score(self, target_users, target_items, seen_SID_weight=True, batch_size=100000):
        """
        :param target_users, must inside model
        :param target_items: SID list  [sid1, sid2, ...]

        :yield:
        """
        # filtering out users who are not in matrix
        # filtered_users = self.collab_model.filter_users(target_users)
        # filtered_users_ids = list(map(lambda vectors: self.collab_model.model.user_item_matrix.user2id[vectors], filtered_users))
        filtered_users_ids = list(map(lambda x: self.collab_model.model.user_item_matrix.user2id[x], target_users))

        # filtering out items who are not in matrix
        filtered_items = self.collab_model.filter_items(target_items)
        filtered_items_ids = list(map(lambda x: self.collab_model.model.user_item_matrix.item2id[x], filtered_items))
        item_vector = self.collab_model.model.item_factors.take(filtered_items_ids, axis=0)

        if len(filtered_users_ids) == 0 or len(filtered_items_ids) == 0:
            logging.debug("not filtered_users_ids or not item_vector")
            return None, None

        if seen_SID_weight:
            filtered_seen_matrix = self.collab_model.model.user_item_matrix.matrix[:, filtered_items_ids]

        for sub_user_list in batch(filtered_users_ids, batch_size):
            user_vectors = self.collab_model.model.user_factors.take(sub_user_list, axis=0)
            scores = user_vectors.dot(item_vector.T)
            if seen_SID_weight:
                subset_users_seen_items = filtered_seen_matrix[sub_user_list, :].todense()
                rows, cols = np.nonzero(subset_users_seen_items)
                scores[rows,cols] = scores[rows,cols] + 100

            yield np.mean(scores, axis=1)

    def rank_characters(self, character_sids_dict, target_user=None, seen_SID_weight=False, show_progress=False):
        if target_user:
            model_users = self.get_users_in_model(target_user)
        else:
            model_users = list(self.collab_model.model.user_item_matrix.user2id.keys())
            logging.info(f"target users = all users in the model: {len(model_users)} users")

        character_scores = []
        r_character_list = []
        #sid_list = []
        # have to make sure all users are in the model before
        pbar = tqdm(character_sids_dict.items()) if show_progress else character_sids_dict.items()
        for character, sids in pbar:
            scores = np.array([])
            for batch_score in self.fet_score(model_users, sids, seen_SID_weight=seen_SID_weight):
                scores = np.concatenate((scores, batch_score), axis=None)

            # skip empty SIDs
            if scores.size != 0:
                character_scores.append(scores)
                r_character_list.append(character)
                #sid_list.append(sids[0])

        character_scores = np.array(character_scores).transpose()
        logging.info(f"character_scores.shape = {character_scores.shape}")

        character_scores = character_scores*-1  # to make argsort rank descendly
        ranked_indice =  np.argsort(character_scores, axis=1)
        r_character_list = np.array(r_character_list)
        #sid_list = np.array(sid_list)

        return model_users, r_character_list[ranked_indice]



def make_characters_alt(pool_path, model_path, top_N):
    character_sids = dict()  # character_id: ['SID0002812', 'SID0002956', ...]
    # "KIDS_SAKUHIN_GROUP_PUBLIC_CODE","KIDS_SAKUHIN_GROUP_NAME","SIDs"
    for line in efficient_reading(pool_path):
        arr = line.rstrip().replace('"', '').split(",")
        character_id = arr[0]
        if arr[-1] != "":
            character_sids[character_id] = character_sids.setdefault(character_id, []) + arr[-1].split("|")

    model = Ranker(model_path)
    user_list, recos = model.rank_characters(character_sids, target_user=None, seen_SID_weight=True,
                                             show_progress=True)
    today = datetime.date.today().strftime("%Y%m%d")
    with open("kids_characters_ALT.csv", "w") as w:
        w.write("user_multi_account_id,character_codes,create_date\n")
        for user_id, reco in zip(user_list, recos):
            w.write(f"{user_id},{'|'.join(reco[:top_N])},{today}\n")

    logging.info("done, output at kids_characters_ALT.csv")


def make_coldstart_characters_alt(pool_path, daily_top_path="daily_top.csv", output="kids_characters_ALT.csv"):
    character_sids = dict()  # character_id: ['SID0002812', 'SID0002956', ...]
    for line in efficient_reading(pool_path):
        arr = line.rstrip().replace('"', '').split(",")
        character_id = arr[0]
        if arr[-1] != "":
            character_sids[character_id] = character_sids.setdefault(character_id, []) + arr[-1].split("|")

    invered_id_character_id_sids_dict = dict()
    for k_id, sids in character_sids.items():
        for sid in sids:
            invered_id_character_id_sids_dict[sid] = k_id

    # rank characters by yesterday's popularity
    character_popularity_cnt = defaultdict(int)
    for line in efficient_reading(daily_top_path):
        arr = line.split(",")
        sid = arr[0]
        if sid not in invered_id_character_id_sids_dict:
            continue

        popularity = arr[2]
        character_popularity_cnt[invered_id_character_id_sids_dict[sid]] += int(popularity)

    coldstart = [x for x,_ in sorted(character_popularity_cnt.items(), key=operator.itemgetter(1), reverse=True)]
    coldstart = coldstart + list(set(character_sids.keys()) - set(coldstart))

    today = datetime.date.today().strftime("%Y%m%d")

    with open(output, "a") as a:
        a.write(f"coldstart,{'|'.join(coldstart)},{today}\n")

    logging.info("done, append to kids_characters_ALT.csv")

if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    if arguments['data_processing']:
        data_processing(arguments["--input"])
    elif arguments['make_alt']:
        make_characters_alt(arguments["--input"], arguments["--model"], int(arguments["<top_N>"]))
    elif arguments['make_coldstart']:
        make_coldstart_characters_alt(arguments["--input"])
    else:
        raise "Unimplementation Error"


