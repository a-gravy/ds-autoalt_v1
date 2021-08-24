"""
implicit BPR for ippan sakuhin
"""
import pandas as pd
import numpy as np
import pickle
import os, sys, tempfile, logging, time
import numpy as np
import time
import csv
import random
from implicit.bpr import BayesianPersonalizedRanking
from scipy.sparse import coo_matrix
from .utils import normalize_path, save_list_to_file, file_to_list

# logging = logging.getlogging(__name__)


class Model:
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


def load_model(path):
    return pickle.load(open(path, "rb"))


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


os.makedirs("./data", exist_ok=True)


class Cleaner:
    def __init__(self, opts):
        """
        v_xiao is for xiao's input, convert name of column to eugene's version
        :param opts:
        """
        self.out_path = opts.get("out_path")
        self.ratings_path = opts.get("ratings_path")
        self.min_sakuhin = opts.get("min_sakuhin", 5)
        print(self.out_path)
        print(self.ratings_path)


    def clean(self):
        logging.info("Initiating data cleanup")

        logging.info(f"Loading ratings file from path {self.ratings_path}")
        ratings = pd.read_csv(self.ratings_path)

        logging.info(f"# of users: {len(ratings.userid.unique())}, # of items: {len(ratings.item.unique())}")

        logging.info("Removing <0 ratings")
        ratings = ratings.query('rating > 0')
        logging.info(f"# of users: {len(ratings.userid.unique())}, # of items: {len(ratings.item.unique())}")

        logging.info(f"Dropping duplicates")
        ratings.drop_duplicates(subset=["userid", "item"], inplace=True)
        logging.info(f"# of users: {len(ratings.userid.unique())}, # of items: {len(ratings.item.unique())}")

        # User watched too few movies, let coldstart handle it.
        logging.info("Dropping users that only watched <= 5 times")
        users_watch_counts = ratings.userid.value_counts()
        users = users_watch_counts[users_watch_counts > self.min_sakuhin].index.tolist()
        ratings = ratings.query('userid in @users')
        logging.info(f"# of users: {len(ratings.userid.unique())}, # of items: {len(ratings.item.unique())}")

        # remove this part or some sakuhins in feature can't have the score (ex: "SID0040314")
        """
        logging.info("Dropping famous items. It is too famous, it does not need to be recommended")
        item_watch_counts = ratings.item.value_counts()
        items = item_watch_counts[item_watch_counts < 10000].index.tolist()
        ratings = ratings.query('item in @items')
        logging.info(f"# of users: {len(ratings.userid.unique())}, # of items: {len(ratings.item.unique())}")
        """
        ratings.to_csv(self.out_path, index=False)
        logging.info(f"Saved clean data to {self.out_path}")


class Duplicates:
    def __init__(self, **kwargs):
        self.__dict__ = kwargs

    def get_uniques_tuples_sougou(self, to_order, start, gap, to_order_rows, context='tochuu', hensei=None):
        uniques = set(np.unique(
            [item for sublist in [to_order[i] for i in to_order_rows[start:start + gap]] for item in sublist]))
        if context == 'end':
            uniques = set(np.unique(
                [item for sublist in [to_order[i] for i in to_order_rows[start:]] for item in sublist]))

        previous_uniques = set()
        if context != 'start':
            previous_uniques = set(np.unique(
                [item for sublist in [to_order[i][:10] for i in to_order_rows[start - 10:start]] for item in sublist]))
        else:
            if hensei is not None:
                previous_uniques = set(hensei)

        uniques = uniques - previous_uniques
        column_advanced_pointer = dict(zip(range(len(to_order)), [0] * len(to_order)))
        column_changed_data = dict(zip(range(len(to_order)), [0] * len(to_order)))
        for j in range(10):
            for i in to_order_rows[start:start + gap]:
                if len(to_order[i]) > j:
                    if to_order[i][j] in uniques:
                        uniques.remove(to_order[i][j])
                        column_advanced_pointer[i] += 1
                        column_changed_data[i] = j
                    else:
                        for k in range(column_advanced_pointer[i], len(to_order[i])):
                            if to_order[i][k] in uniques:
                                uniques.remove(to_order[i][k])
                                column_advanced_pointer[i] = k + 1
                                column_changed_data[i] = j
                                to_order[i][j], to_order[i][k] = to_order[i][k], to_order[i][j]
                                if k < len(to_order[i]):
                                    shift_pos = random.randint(k, len(to_order[i]) - 1)
                                    to_order[i][k], to_order[i][shift_pos] = to_order[i][shift_pos], to_order[i][k]
                                break

    def get_uniques_by_tag(self, to_order, to_order_tags, tag, henpick=0):
        to_order_rows = np.array(range(len(to_order)))
        if tag != 'HOME':
            to_order_rows = np.where(to_order_tags == tag)[0]
        batch_length = round(to_order_rows.shape[0] / 10)
        henpick = None
        # if henpick == 1:
        #    henpick_list = hen_pick_list[tag]
        for i in range(batch_length):
            # print('batch : {}'.format(i))
            if i == 0:
                self.get_uniques_tuples_sougou(
                    to_order=to_order, start=0, gap=10, to_order_rows=to_order_rows,
                    context='start')  # , hensei=hen_pick_list)
            elif i == batch_length - 1:
                self.get_uniques_tuples_sougou(
                    to_order, (i + 1) * 10, 10, to_order_rows, context='end')
            else:
                self.get_uniques_tuples_sougou(
                    to_order, (i + 1) * 10 + 1, 10, to_order_rows)

    def rerank_all(self, to_order, to_order_tags, tags=['HOME'], henpick=0):
        for tag in tags:
            self.get_uniques_by_tag(to_order, to_order_tags, tag, henpick=henpick)


class UserItemRatingMatrix:
    """
    make input for implict bpr algorithm

    input file is a csv in format:
    "userid",        "item", "rating"
    C0000000056, SID0031788,     1
    ---------------
    output: a dict {
        "user2id": user2id,  # dict {PM015212809:0, ... }
        "item2id": item2id,  # dict {SID0042488:0, ... }
        "id2item": id2item,
        "id2user": id2user,
        "matrix": matrix     # the input for bpr trainer
        }

    """

    def __init__(self, **kwargs):
        self.__dict__ = kwargs

    @staticmethod
    def from_ratings_file(path, quiet=False, target_items=None, target_users=None, user_col="userid", item_col="item",
                          rating_col="rating"):
        """
        Reads ratings file without using Panda(to save memory). ASSUMES that there are no duplicate rows in the file!
        """
        ctr = 0

        users = []
        items = []
        ratings = []

        if target_items is not None: target_items = set(target_items)
        if target_users is not None: target_users = set(target_users)

        with open(path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ctr += 1
                userid, item, rating = row[user_col], row[item_col], row[rating_col]
                include_item = target_items is None or item in target_items
                include_user = target_users is None or userid in target_users
                if include_item and include_user:
                    users.append(userid)
                    items.append(item)
                    ratings.append(rating)

                if not quiet and ctr % 10000 == 0:
                    print(ctr, end="\r")

        # map userid/item2id to integer from 0 ~ N
        user2id = {o: i for i, o in enumerate(list(set(users)))}
        item2id = {o: i for i, o in enumerate(list(set(items)))}
        id2user = {v: k for k, v in user2id.items()}
        id2item = {v: k for k, v in item2id.items()}

        logging.info(f"# of users: {len(user2id.keys())}, # of items: {len(item2id.keys())}")

        if len(users) == 0:
            logging.info("Processed the file but all items were filtered out. Returning None")
            return None

        logging.info(f"Creating matrix")

        row = [user2id[user] for user in users]
        col = [item2id[item] for item in items]
        data = [float(rating) for rating in ratings]

        matrix = coo_matrix((data, (row, col))).tocsr()
        logging.info(f"Matrix shape: {matrix.shape}")

        user_item_rating_matrix = UserItemRatingMatrix(**{
            "user2id": user2id,
            "item2id": item2id,
            "id2item": id2item,
            "id2user": id2user,
            "matrix": matrix
        })
        return user_item_rating_matrix

    def get_user_items(self, user):
        """
        Get nonzero items from the matrix
        """
        if user in self.user2id:
            itemids = set(self.matrix[self.user2id[user]].nonzero()[1])
            return [self.id2item[itemid] for itemid in itemids]
        else:
            return []

    def convert_items_to_index(self, items):
        return [self.item2id[item] for item in items]


class Trainer:
    """
    BPR trainer

    input = UserItemRatingMatrix,
    output = dict {
        "bpr_model":bpr,
        "user_item_matrix": user_item_matrix,
        "user_factors": bpr.user_factors,  # Array of latent factors for each user in the training set
        "item_factors": bpr.item_factors  # Array of latent factors for each item in the training set
    }
    """

    def __init__(self, opts):
        self.out_path = normalize_path(opts.get("out_path"))
        self.ratings_path = normalize_path(opts.get("ratings"))
        self.algo = opts.get("algo")
        self.supported_users_list_path = opts.get("supported_users_list")
        self.supported_items_list_path = opts.get("supported_items_list")
        self.quiet = opts.get("quiet") == "True"
        print("self.out_path  ", self.out_path)
        print("self.ratings_path  ", self.ratings_path)
        print("self.algo  ", self.algo)
        print("self.supported_users_list_path  ", self.supported_users_list_path)
        print("self.supported_items_list_path  ", self.supported_items_list_path)
        print("self.quiet  ", self.quiet)

    def train_implicitbpr(self):

        logging.info("Loading ratings file")
        user_item_matrix = UserItemRatingMatrix.from_ratings_file(self.ratings_path, quiet=self.quiet)

        logging.info("Starting training")
        bpr = BayesianPersonalizedRanking(factors=128, verify_negative_samples=True)
        # get "matrix" (the item-user data) from user_item_matrix
        bpr.fit(user_item_matrix.matrix.T.tocsr())

        logging.info("Training has finished.")

        # this format is for speeding up
        model = Model(**{
            "bpr_model": bpr,
            "user_item_matrix": user_item_matrix,
            "user_factors": bpr.user_factors,  # Array of latent factors for each user in the training set
            "item_factors": bpr.item_factors  # Array of latent factors for each item in the training set
        })
        pickle.dump(model, open(self.out_path, "wb"))

        logging.info(f"Saved model to {self.out_path}")
        return model

    def train(self):
        logging.info(f"Initiating algorithm for algo:{self.algo}")
        if self.algo == "implicit":
            model = self.train_implicitbpr()
        else:
            print("Algorithm is not yet supported")

        # TODO: check redundancy
        if model:
            logging.info(f"Saving supported users to:{self.supported_users_list_path}")
            save_list_to_file(list(model.user_item_matrix.user2id.keys()), self.supported_users_list_path)

            logging.info(f"Saving supported items to:{self.supported_items_list_path}")
            save_list_to_file(list(model.user_item_matrix.item2id.keys()), self.supported_items_list_path)

            logging.info("Done")


def feature_ranking(model_path, target_users_path, output_file_name,
                    output_toppick_file_name, watched_list_rerank_path,
                    ucb_rerank_path, toppick_rerank_path,
                    feature_list_rerank_path, MINIMAL_NB_SAKUHIN, NB_OUTPUT_FEATURE,
                    OVERLAPPED_RATIO_THRESHOLD):
    start_t = time.time()
    model = load_model(model_path)
    target_users = file_to_list(target_users_path)

    ######################################### For creating a dictionary for feature_lists
    ff = pd.read_csv(feature_list_rerank_path)

    def cut_sakuhin_row(row):
        return row['sakuhin_codes'].split('|')

    ff['sakuhin_codes'] = ff.apply(lambda row: cut_sakuhin_row(row), axis=1)
    d_feature_id_s_list = dict(zip(ff["feature_public_code"], ff['sakuhin_codes']))
    df_feature_index = dict(zip(ff["feature_public_code"], ff.index))
    df_index_feature = dict(zip(ff.index, ff["feature_public_code"]))
    df_genre_tag_code = dict(zip(ff.feature_public_code, ff.genre_tag_code))
    df_platform = dict(zip(ff.feature_public_code, ff.platform))
    df_film_rating_order = dict(zip(ff.feature_public_code, ff.film_rating_order))
    df_feature_public_flg = dict(zip(ff.feature_public_code, ff.feature_public_flg))
    df_feature_display_flg = dict(zip(ff.feature_public_code, ff.feature_display_flg))
    df_feature_home_display_flg = dict(zip(ff.feature_public_code, ff.feature_home_display_flg))
    df_feature_public_start_datetime = dict(zip(ff.feature_public_code, ff.feature_public_start_datetime))
    df_feature_public_end_datetime = dict(zip(ff.feature_public_code, ff.feature_public_end_datetime))
    saku_number_egalizator = ff['num_elements'].values

    feat_np = np.full((ff.shape[0], len(model.user_item_matrix.item2id)), 0)
    for fet in d_feature_id_s_list.keys():
        for sids in d_feature_id_s_list[fet]:
            if sids in model.user_item_matrix.item2id.keys():
                feat_np[df_feature_index[fet]][model.user_item_matrix.item2id[sids]] = 1

    dtop = pd.read_csv(toppick_rerank_path)
    dtop['sakuhin_codes'] = dtop.apply(lambda row: cut_sakuhin_row(row), axis=1)
    dtop_feature_id_s_list = dict(zip(dtop["feature_public_code"], dtop['sakuhin_codes']))
    dtop_feature_index = dict(zip(dtop["feature_public_code"], dtop.index))
    dtop_index_feature = dict(zip(dtop.index, dtop["feature_public_code"]))
    print(dtop_index_feature)

    ############################ Create toppick x sids matrix (just one toppick)
    top_np = np.full((dtop.shape[0], len(model.user_item_matrix.item2id)), 0)
    for fet in dtop_feature_id_s_list.keys():
        for sids in dtop_feature_id_s_list[fet]:
            if sids in model.user_item_matrix.item2id.keys():
                top_np[dtop_feature_index[fet]][model.user_item_matrix.item2id[sids]] = 1

    ####################################### Create dico user_id : [watched sids]
    list_watched = file_to_list(watched_list_rerank_path)
    dict_watched_sakuhin = {}
    for i in list_watched:
        userid, sids = i.split(',')
        user_matrix_id = model.user_item_matrix.user2id.get(userid, -1)
        if user_matrix_id != -1:
            sid = sids.split('|')
            dict_watched_sakuhin[user_matrix_id] = sid
    logging.info('dict_watched_sakuhin len : {}'.format(len(dict_watched_sakuhin)))
    del list_watched

    dfetucb = pd.read_csv(ucb_rerank_path)
    d_fet_ucb = dict(zip(dfetucb.feature_public_code, dfetucb.ucb_score))
    ############################################# FET Matrix completed
    with open("./results/" + output_toppick_file_name, "w") as csvfiletop:
        fieldnames_top = [
            'user_multi_account_id', 'toppick_type_no', 'sakuhin_codes',
            'toppick_title', 'toppick_comment', 'alg_label1', 'alg_label2'
        ]
        writer_top = csv.DictWriter(csvfiletop, fieldnames=fieldnames_top)
        writer_top.writeheader()

        ############################################# FET Matrix completed
        with open("./results/" + './streamsets.csv', "w") as csvfile_top_ss:
            fieldnames_top_stream = [
                "user_multi_account_id",
                "feature_public_codes",
                "feature_scores",
                "sakuhin_codes"
            ]
            writer_stream = csv.DictWriter(csvfile_top_ss, fieldnames=fieldnames_top_stream)
            # writer.writeheader()
            ############################################# FET Matrix completed
            with open('./results/' + output_file_name, "w") as csvfile:
                fieldnames = [
                    "user_multi_account_id", "feature_public_code", "sakuhin_codes",
                    "feature_score", "feature_ranking", "genre_tag_code", "platform",
                    "film_rating_order", "feature_public_flg", "feature_display_flg",
                    "feature_home_display_flg", "feature_public_start_datetime",
                    "feature_public_end_datetime"
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                ################### taking the valid users from the target_users
                today_users = list(set(target_users))
                number_of_batches = round(len(today_users) / 10000)
                batch_number = 0
                logging.info('number of batches to do : {}'.format(number_of_batches))
                today_users = np.array(today_users)
                for first_batch in np.array_split(today_users, number_of_batches):
                    logging.info("batch {} of {} : {} ".format(
                        batch_number,
                        number_of_batches,
                        float(batch_number) / number_of_batches))
                    batch_number += 1
                    first_batch = list(map(lambda x: model.user_item_matrix.user2id[
                        x] if x in model.user_item_matrix.user2id.keys() else 'coldstart', first_batch))
                    first_batch = list(filter(lambda x: x != 'coldstart', first_batch))
                    logging.info('number of users in current batch : {}'.format(
                        len(first_batch)))

                    user_factors = model.user_factors.take(first_batch, axis=0)
                    user_items_score = user_factors.dot(model.bpr_model.item_factors.T)

                    # logging.info("Populate watched sakuhin for batch users")
                    # for i in range(len(first_batch)):
                    #    uid = first_batch[i]
                    #    if uid in dict_watched_sakuhin:
                    #        for sids in dict_watched_sakuhin[uid]:
                    #            kid = model.user_item_matrix.item2id.get(sids,-1)
                    #            if kid != -1:
                    #                user_items_score[i,kid] = 0

                    user_features_score = user_items_score.dot(feat_np.T)
                    user_features_score = np.multiply(saku_number_egalizator, user_features_score)
                    user_toppick_score = user_items_score.dot(top_np.T)

                    ####### Getting the top 200 features for the first batch user
                    user_top200_feature = (-user_features_score).argsort(axis=1)[:, :100]
                    user_top200_feature_val = np.take_along_axis(
                        user_features_score,
                        user_top200_feature, axis=1)

                    f = lambda x: df_index_feature[x]
                    tofet = np.vectorize(f)

                    ################# need to add the sids that are not in the
                    # feature right now but that are in the bpr model (to get
                    # the same dimension)
                    for counter, uid in enumerate(first_batch):  # for every target user
                        # print(counter)
                        # start_t = time.time()
                        if counter % 500 == 0:
                            logging.info("{}/{} {} ".format(
                                counter + 1,
                                len(first_batch),
                                float(counter) / len(first_batch)))

                        feature_public_code_list = []
                        feature_score_list = []
                        sakuhin_ranking_list = []

                        c_uid = user_items_score[counter]
                        user_features = list(zip(
                            tofet(user_top200_feature[counter:counter + 1, :]).flatten(),
                            user_top200_feature_val[counter:counter + 1, :].flatten()))
                        user_features = list(filter(
                            lambda x: x[0] != 'unknown',
                            user_features))

                        def rerank_ucb_bis(l1):
                            argsortons = np.array(list(map(lambda x: d_fet_ucb[x[0]] if x[0] in d_fet_ucb else 0, l1)))
                            new_order = (-argsortons).argsort()
                            return [l1[x] for x in new_order]

                        user_features = rerank_ucb_bis(user_features)
                        user_fet_sids = list(map(
                            lambda x: list(map(
                                lambda y: model.user_item_matrix.item2id.get(y, -1),
                                d_feature_id_s_list[x[0]])),
                            user_features))
                        user_multi_acc_id = model.user_item_matrix.id2user[uid]

                        user_toppick_sids = list(map(
                            lambda x: list(map(
                                lambda y: model.user_item_matrix.item2id.get(y, -1),
                                dtop_feature_id_s_list[x])),
                            ['toppick_1']))

                        user_watched = set()
                        if uid in dict_watched_sakuhin:
                            user_watched = set(dict_watched_sakuhin.pop(int(uid)))

                        def get_uniques_tuples(test):
                            uniques = np.unique([item[0] for sublist in test for item in sublist])
                            uniques = set(uniques) - user_watched
                            column_advanced_pointer = dict(zip(range(len(test)), [0] * len(test)))
                            column_changed_data = dict(zip(range(len(test)), [0] * len(test)))
                            for j in range(4):
                                for i in range(len(test)):
                                    if test[i][j][0] in uniques:
                                        uniques.remove(test[i][j][0])
                                        column_advanced_pointer[i] += 1
                                        column_changed_data[i] = j
                                    else:
                                        for k in range(column_advanced_pointer[i], len(test[i])):
                                            if test[i][k][0] in uniques:
                                                uniques.remove(test[i][k][0])
                                                column_advanced_pointer[i] = k + 1
                                                column_changed_data[i] = j
                                                test[i][j], test[i][k] = test[i][k], test[i][j]
                                                test[i][k], test[i][-1] = test[i][-1], test[i][k]
                                                break
                            return test

                        ########### sort sakuhin inside of each features by bpr score
                        list_of_list = []
                        list_of_fet = []
                        for j in range(len(user_fet_sids)):
                            fet_pub_code, fet_pub_score = user_features[j]
                            got_sids = c_uid.take(user_fet_sids[j])
                            g_sids = (-got_sids).argsort()
                            g_sids_val = np.take_along_axis(got_sids, g_sids, axis=0)
                            sid_sids = np.array(user_fet_sids[j]).take(g_sids)
                            tuplee = list(zip(list(map(lambda x: model.user_item_matrix.id2item.get(x, -1), sid_sids)),
                                              np.take_along_axis(got_sids, g_sids, axis=0)))
                            list_of_fet.append((fet_pub_code, fet_pub_score))
                            list_of_list.append(tuplee)

                        ############### sort sakuhin inside of toppick by bpr score
                        top_list = []
                        for j in range(len(user_toppick_sids)):
                            got_sids = c_uid.take(user_toppick_sids[j])
                            g_sids = (-got_sids).argsort()
                            g_sids_val = np.take_along_axis(got_sids, g_sids, axis=0)
                            sid_sids = np.array(user_toppick_sids[j]).take(g_sids)
                            tuplee = list(zip(
                                list(map(
                                    lambda x: model.user_item_matrix.id2item.get(x, -1),
                                    sid_sids)),
                                np.take_along_axis(got_sids, g_sids, axis=0)))
                            top_list.append(tuplee)

                        ########## anata_osusume top 50 = toppick 1; 51-100 = toppick 2
                        wan_tsuu = [top_list[0][:50], top_list[0][50:100]]

                        # def rerank_ucb(l1,l2):
                        #    argsortons = np.array(list(map(lambda x : d_fet_ucb[x[0]] if x[0] in d_fet_ucb else 0,l1)))
                        #    new_order = (-argsortons).argsort()
                        #    new_l1 = [ l1[x] for x in new_order]
                        #    new_l2 = [ l2[x] for x in new_order]
                        #    return new_l1, new_l2

                        ########################## reranking using the ucb score from GP
                        # list_of_fet, list_of_list = rerank_ucb(list_of_fet,list_of_list)

                        ############################### getting rid of the juufuku
                        list_of_list = get_uniques_tuples(wan_tsuu + list_of_list[:75])
                        top_list = list_of_list[:2]
                        list_of_list = list_of_list[2:]
                        ############################### Start writing reco to rec_general
                        for i in range(len(list_of_list)):
                            sakuhin_codes = list(filter(lambda x: x[0] != -1, list_of_list[i]))
                            sakuhin_codes = '|'.join(list(map(lambda x: x[0], sakuhin_codes)))
                            fet_public_code = list_of_fet[i][0]
                            writer.writerow({
                                "user_multi_account_id": user_multi_acc_id,
                                "feature_public_code": fet_public_code,
                                "sakuhin_codes": sakuhin_codes,
                                "feature_score": list_of_fet[i][1],
                                "feature_ranking": i,
                                "genre_tag_code": df_genre_tag_code[fet_public_code],
                                "platform": df_platform[fet_public_code],
                                "film_rating_order": df_film_rating_order[fet_public_code],
                                "feature_public_flg": df_feature_public_flg[fet_public_code],
                                "feature_display_flg": df_feature_display_flg[fet_public_code],
                                "feature_home_display_flg": df_feature_home_display_flg[fet_public_code],
                                "feature_public_start_datetime": df_feature_public_start_datetime[fet_public_code],
                                "feature_public_end_datetime": df_feature_public_end_datetime[fet_public_code]
                            })

                        ############################### Start writing reco to rec_toppick
                        for i in range(len(top_list)):
                            sakuhin_codes = list(filter(lambda x: x[0] != -1, top_list[i]))
                            sakuhin_codes = '|'.join(list(map(lambda x: x[0], sakuhin_codes)))
                            writer_top.writerow({
                                'user_multi_account_id': user_multi_acc_id,
                                'toppick_type_no': i + 1,
                                'sakuhin_codes': sakuhin_codes,
                                'toppick_title': '',
                                'toppick_comment': '',
                                'alg_label1': 'MF',
                                'alg_label2': 'MF'
                            })

                        ############################## Start old streamsets way
                        big_bulk = []
                        for i in range(min(50, len(list_of_list))):
                            sakuhin_codes = list(filter(lambda x: x[0] != -1, list_of_list[i]))
                            sakuhin_codes = ':'.join(list(map(lambda x: x[0], sakuhin_codes))[:50])
                            big_bulk.append(sakuhin_codes)

                        writer_stream.writerow({
                            "user_multi_account_id": user_multi_acc_id,
                            "feature_public_codes": ','.join([x[0] for x in list_of_fet]),
                            "feature_scores": '|'.join(["{0:.4f}".format(x[1]) for x in list_of_fet]),
                            "sakuhin_codes": '|'.join(big_bulk)
                        })
            logging.info("entire process takes {} ".format(time.time() - start_t))


def coldstart_ranking(output_file_name, coldstart_result_path,
                      feature_list_rerank_path, out_starship_path,
                      create_date=0):
    start_t = time.time()
    # target_users = file_to_list('./data/new_coldstart.csv')

    dff = pd.read_csv(coldstart_result_path).fillna("")
    # dff = pd.read_sql(query_feats, prod_engine)
    users_fet_list = dff.feature_public_code.values
    users_fet_score = dff.feature_score.values
    # users_fet_sid = list(map(lambda x : x.split('|'),dff.sakuhin_codes.values))
    users_fet_tag = dff.tag_name.values
    users_fet_rnk = dff.feature_rnk.values
    target_users = 'coldstart'

    ######################################### For creating a dictionary for feature_lists
    ff = pd.read_csv(feature_list_rerank_path).fillna("")

    def cut_sakuhin_row(row):
        return row['sakuhin_codes'].split('|')

    ff['sakuhin_codes'] = ff.apply(lambda row: cut_sakuhin_row(row), axis=1)
    d_feature_id_s_list = dict(zip(ff["feature_public_code"], ff['sakuhin_codes']))
    df_feature_index = dict(zip(ff["feature_public_code"], ff.index))
    df_index_feature = dict(zip(ff.index, ff["feature_public_code"]))
    df_genre_tag_code = dict(zip(ff.feature_public_code, ff.genre_tag_code))
    df_platform = dict(zip(ff.feature_public_code, ff.platform))
    df_film_rating_order = dict(zip(ff.feature_public_code, ff.film_rating_order))
    df_feature_public_flg = dict(zip(ff.feature_public_code, ff.feature_public_flg))
    df_feature_display_flg = dict(zip(ff.feature_public_code, ff.feature_display_flg))
    df_feature_home_display_flg = dict(zip(ff.feature_public_code, ff.feature_home_display_flg))
    df_feature_public_start_datetime = dict(zip(ff.feature_public_code, ff.feature_public_start_datetime))
    df_feature_public_end_datetime = dict(zip(ff.feature_public_code, ff.feature_public_end_datetime))

    users_fet_sid = list(map(
        lambda x: d_feature_id_s_list[x] if x in d_feature_id_s_list else None,
        users_fet_list))
    sids = np.array(users_fet_sid)
    tags = np.array(users_fet_tag)

    tag_list = [
        'HOME', 'アニメ', 'キッズ', 'ドキュメンタリー', 'バラエティ',
        '国内ドラマ', '洋画', '海外ドラマ', '邦画',
        '韓流・アジアドラマ', '音楽・アイドル'
    ]

    duplicator = Duplicates()
    duplicator.rerank_all(sids, tags, tag_list)

    ############################################# FET Matrix completed
    with open(output_file_name, "w") as csvfile:
        columns = [
            'user_multi_account_id',
            'feature_public_code',
            'sakuhin_codes',
            'feature_score',
            'feature_ranking',
            'genre_tag_code',
            'platform',
            'film_rating_order',
            'feature_public_flg',
            'feature_display_flg',
            'feature_home_display_flg',
            'feature_public_start_datetime',
            'feature_public_end_datetime',
            'to_del'
        ]

        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        with open(out_starship_path, "w") as csvstarshipfile:
            columns_starship = [
                'user_multi_account_id',
                'feature_public_code',
                'sakuhin_codes',
                'feature_score',
                'feature_ranking',
                'genre_tag_code',
                'platform',
                'film_rating_order',
                'feature_public_flg',
                'feature_display_flg',
                'feature_home_display_flg',
                'feature_public_start_datetime',
                'feature_public_end_datetime',
                'create_date'
            ]

            starwriter = csv.DictWriter(csvstarshipfile, fieldnames=columns_starship)
            starwriter.writeheader()

            for i in range(len(users_fet_list)):
                to_del = 1 if i == 0 else 0
                sakuhin_codes = '|'.join(sids[i])
                fet_public_code = users_fet_list[i]
                fet_score = users_fet_score[i]
                rankingu = users_fet_rnk[i]
                writer.writerow({
                    "user_multi_account_id": 'coldstart',
                    "feature_public_code": fet_public_code,
                    "sakuhin_codes": sakuhin_codes,
                    "feature_score": fet_score,
                    "feature_ranking": rankingu,
                    "genre_tag_code": df_genre_tag_code[fet_public_code] if df_genre_tag_code[fet_public_code] != 'nan' else '',
                    "platform": df_platform[fet_public_code],
                    "film_rating_order": df_film_rating_order[fet_public_code],
                    "feature_public_flg": df_feature_public_flg[fet_public_code],
                    "feature_display_flg": df_feature_display_flg[fet_public_code],
                    "feature_home_display_flg": df_feature_home_display_flg[fet_public_code],
                    "feature_public_start_datetime": df_feature_public_start_datetime[fet_public_code],
                    "feature_public_end_datetime": df_feature_public_end_datetime[fet_public_code],
                    "to_del": to_del
                })

                starwriter.writerow({
                    "user_multi_account_id": 'coldstart',
                    "feature_public_code": fet_public_code,
                    "sakuhin_codes": sakuhin_codes,
                    "feature_score": fet_score,
                    "feature_public_flg": df_feature_public_flg[fet_public_code],
                    "feature_display_flg": df_feature_display_flg[fet_public_code],
                    "feature_home_display_flg": df_feature_home_display_flg[fet_public_code],
                    "feature_public_start_datetime": df_feature_public_start_datetime[fet_public_code],
                    "feature_public_end_datetime": df_feature_public_end_datetime[fet_public_code],
                    "film_rating_order": df_film_rating_order[fet_public_code],
                    "genre_tag_code": df_genre_tag_code[fet_public_code] if df_genre_tag_code[fet_public_code] != 'nan' else '',
                    "create_date": int(create_date)
                })

def rerank(model,
           target_users=None,
           target_items=None,
           filter_already_liked_items=True,
           top_n=200,  # -1 means all
           batch_size=500):
    """
    do basic reranking & filtering

    :param target_users: UID list, [uid1, uid2, ...]; None means all users in matrix
    :param target_items: SID list  [sid1, sid2, ...]; None means all items in matrix
    :param filter_already_liked_items: removed the nonzeros item in matrix, set False if you have special filtering
    :param N: minimum nb of output
    :param batch_size:
    :yield: uid, sid list, score list
    """

    # user id -> matrix index
    if target_users is None:
        target_users_index_list = list(model.user_item_matrix.user2id.values())
    else:
        target_users_index_list = [model.user_item_matrix.user2id.get(user) for user in target_users if
                                   model.user_item_matrix.user2id.get(user) is not None]

    # make target matrix, which contains target items only
    if target_items is None:
        # SID -> matrix index
        target_items2actualid = {i: i for i in range(model.user_item_matrix.matrix.shape[1])}
        target_matrix = model.user_item_matrix.matrix
        item_factors = model.item_factors
    else:
        target_items_index_list = list({model.user_item_matrix.item2id.get(item) for item in target_items if
                                        model.user_item_matrix.item2id.get(item) is not None})
        # matrix_index -> target_matrix_index
        target_items2actualid = {i: target for i, target in enumerate(target_items_index_list)}
        # target_matrix[nb of user, nb of target items], contains target items only by target_matrix_index
        target_matrix = model.user_item_matrix.matrix[:, target_items_index_list]
        item_factors = model.item_factors[target_items_index_list, :]

    # matrix operation on batch of user
    for uidxs in batch(target_users_index_list, batch_size):

        # for uidxs(a batch of user), get the score for target items,
        scores = model.user_factors[uidxs].dot(item_factors.T)
        rows = target_matrix[uidxs]

        for i, uid in enumerate(uidxs):  # for each user
            # get the top N
            nonzeros_in_row = set(rows[i].nonzero()[1]) if filter_already_liked_items else set()
            count = max(top_n + len(nonzeros_in_row), top_n*2)  # to make sure we have enough items for recommendation
            if count < len(scores[i]):  # scores[i].shape = (nb of item, )
                if top_n == -1:  # output all results
                    ids = np.argsort(scores[i])
                else:
                    ids = np.argpartition(scores[i], -count)[-count:]
                best = sorted(zip(ids, scores[i][ids]), key=lambda x: -x[1])
            else:
                best = sorted(enumerate(scores[i]), key=lambda x: -x[1])

            reranked_targer_items = [(index, score) for index, score in best if index not in nonzeros_in_row]
            score_list = [score for index, score in reranked_targer_items]
            # target matrix index -> matrix index -> item id
            reranked_item_indexs = [model.user_item_matrix.id2item[target_items2actualid[index]]
                                    for index, score in reranked_targer_items]

            yield model.user_item_matrix.id2user[uid], reranked_item_indexs, score_list


class Recommender:
    """
    make recommendation,
    using rerank(), we make customized functions for different purposes
    * recommend_all : make recommendation on all items
    * toppick : make recommendation on new and popular items
    """

    def __init__(self, opts):
        os.makedirs("./data", exist_ok=True)
        self.model_path = normalize_path(opts.get("model"), error_if_not_exist=True)
        self.model = load_model(self.model_path)
        logging.info("load models for {} users and {} items".format(len(self.model.user_item_matrix.user2id),
                                                                    len(self.model.user_item_matrix.item2id)))

        self.out_path = normalize_path(opts.get("out"))
        self.out_starship_path = normalize_path(opts.get("out_starship"))
        self.create_date = int(opts.get("create_date",0))
        # a list of target userid(ex: PM015212809); None = all users
        self.target_users_path = normalize_path(opts.get("target_users", None))
        # a list of target itemid(ex: SID0041816); None = all items
        self.target_items_path = normalize_path(opts.get("target_items", None))
        self.filter_items_path = normalize_path(opts.get("filter_items", None))
        self.watched_list_rerank_path = normalize_path(opts.get("watched_list_rerank", None))
        self.series_rel_path = normalize_path(opts.get("series_rel", None))
        self.nb_reco = int(opts.get("nb_reco", 200))
        self.quiet = opts.get("quiet") == "True"
        self.label = opts.get("label", None)

        self.target_users = file_to_list(self.target_users_path) if self.target_users_path is not None else None

        self.target_items = file_to_list(self.target_items_path) if self.target_items_path is not None else None
        logging.info("make recommendation for {} users and {} items".format(
            len(self.target_users) if self.target_users else len(self.model.user_item_matrix.user2id),
            len(self.target_items) if self.target_items else len(self.model.user_item_matrix.item2id)
        ))

        self.filter_items = file_to_list(self.filter_items_path) if self.filter_items_path is not None else {}

        self.dict_watched_sakuhin = {}
        if self.watched_list_rerank_path is not None:
            list_watched = file_to_list(self.watched_list_rerank_path)
            for i in list_watched:
                userid, sids = i.split(',')
                self.dict_watched_sakuhin[userid] = sids.split("|")
            del list_watched

        self.series_dict = {}
        if self.series_rel_path:
            series_df = pd.read_csv(self.series_rel_path)
            for row in series_df.iterrows():
                self.series_dict[row[1]["sakuhin_public_code"]] = row[1]["series_id"]
            del series_df

    def recommend_all(self):
        with open(self.out_path, 'w') as csvfile:
            # with open('/app/bpr_score_all_new.csv, 'w') as csvfile_new:
            for i, (uid, reranked_list, score_list) in enumerate(
                    rerank(model=self.model, target_users=self.target_users, target_items=self.target_items,
                           filter_already_liked_items=False, N=800)):

                if i % 1000 == 0:
                    total_nb = len(self.target_users) if self.target_users else len(self.model.user_item_matrix.user2id)
                    logging.info("progress {}/{} = {:.1f}%".format(i, total_nb, float(i) / total_nb * 100))

                assert len(reranked_list) == len(score_list), "bug len(reranked_list):{} != len(score_list):{}".format(
                    len(reranked_list), len(score_list))

                nb_written = 0
                # TODO: remove label
                for sid, score in zip(reranked_list, score_list):
                    if sid not in set(self.filter_items) and sid not in set(self.dict_watched_sakuhin.get(uid, [])):
                        to_write = "{},{},{:.3f},{}\n".format(uid, sid, score, self.label) if self.label else "{},{},{:.3f}\n".format(uid, sid, score)
                        csvfile.write(to_write)

                        nb_written += 1
                        if nb_written >= self.nb_reco:
                            break

    def toppick(self):
        with open(self.out_path, 'w') as csvfile:
            for i, (uid, reranked_list, score_list) \
                    in enumerate(
                rerank(model=self.model, target_users=self.target_users, target_items=self.target_items,
                       filter_already_liked_items=False, N=800)):

                if i % 1000 == 0:
                    total_nb = len(self.target_users) if self.target_users else len(self.model.user_item_matrix.user2id)
                    logging.info("progress {}/{} = {:.1f}%".format(i, total_nb, float(i) / total_nb * 100))

                assert len(reranked_list) == len(score_list), "bug len(reranked_list):{} != len(score_list):{}".format(
                    len(reranked_list), len(score_list))

                # TODO: same series may kill too many items?
                """
                if self.series_dict:
                    series_recorder = set()
                    r_reranked_list, r_score_list = [], []
                    for sid, score in zip(reranked_list, score_list):
                        series_id = self.series_dict.get(sid, None)
                        if series_id:
                            if series_id in series_recorder:
                                continue
                            else:
                                series_recorder.add(series_id)
                                r_reranked_list.append(sid)
                                r_score_list.append(score)
                else:
                    r_reranked_list, r_score_list = reranked_list, score_list
                """
                # TODO: old output format for GP, remove it when stable
                """
                nb_written = 0
                for sid, score in zip(reranked_list, score_list):
                    if sid not in set(self.filter_items) and sid not in set(self.dict_watched_sakuhin.get(uid, [])):
                        csvfile.write("{},{},{:.3f},{}\n".format(uid, sid, score, self.label))

                        nb_written += 1
                        if nb_written >= self.nb_reco:
                            break
                """

                sid_score = [(sid, score) for sid, score in zip(reranked_list, score_list) if
                             sid not in set(self.filter_items) and sid not in set(self.dict_watched_sakuhin.get(uid, []))]

                r_reranked_list = [sid for sid, _ in sid_score]
                r_score_list = ["{:.3f}".format(score) for _, score in sid_score]
                csvfile.write(
                    "{},1,{},,,MF,MF,{}\n".format(uid, "|".join(r_reranked_list[:50]), "|".join(r_score_list[:50])))
                csvfile.write(
                    "{},2,{},,,MF,MF,{}\n".format(uid, "|".join(r_reranked_list[50:100]),
                                                  "|".join(r_score_list[50:100])))

    def _squeeze_by_series(self):  # keep the same series sakuhin only one in the list
        rec = pd.read_csv(self.out_path, names=["user", "sakuhin_public_code", "score"])
        series_df = pd.read_csv(self.series_rel_path)
        j = pd.merge(rec, series_df, how='left', on="sakuhin_public_code")
        del rec
        del series_df
        du = j.duplicated(subset=["user", "series_id"], keep='first')
        j = j[~du].drop("series_id", axis=1)
        j.reset_index(drop=True, inplace=True)
        j.to_csv(self.out_path, index=False, header=False, float_format='%.7f')
        logging.info("remove redundant series sakuhins, total len to {}".format(len(j)))

    def _add_label(self, label):
        os.rename(self.out_path, "tmp")
        with open(self.out_path, "w") as w:
            with open("tmp", "r") as r:
                while True:
                    line = r.readline()
                    if line:
                        w.write("{},{}\n".format(line.rstrip(), label))
                    else:
                        break
        logging.info("add label:{} to {}".format(label, self.out_path))


    def make_postplay_list(self, top_k=10):
        with open(self.out_path, "w") as w:
            with open(self.out_starship_path, "w") as csvstarshipfile:
                columns_starship = ['uid', 'rec_sakuhin_codes', 'create_date']
                starwriter = csv.DictWriter(csvstarshipfile, fieldnames=columns_starship)
                starwriter.writeheader()
                for i in range(len(self.model.user_item_matrix.id2item)):
                    similar_items = self.model.bpr_model.similar_items(i, top_k + 1)  # +1, since including itself
                    similar_items = similar_items[1:]
                    post_play_list = []
                    for item_index, score in similar_items:
                        post_play_list.append(self.model.user_item_matrix.id2item[item_index])
                    w.write("{},{}\n".format(self.model.user_item_matrix.id2item[i], "|".join(post_play_list)))
                    starwriter.writerow({
                        "uid": self.model.user_item_matrix.id2item[i],
                        "rec_sakuhin_codes": "|".join(post_play_list),
                        "create_date": self.create_date
                    })
                    if not self.quiet and i % 100 == 0:
                        logging.info("{}/{}".format(i, len(self.model.user_item_matrix.id2item)))

    def make_filmarks_postplay(self, top_k=10):
        """
        special postplay for kensaku,
        output format: filmarks_id,sakuhin_public_code,score,dt
        """
        # TODO: for controlling nb of kensaku sakuhins
        nb_kensaku = 5000
        counter = 0

        popular_filmarks_id = []
        with open("filmarks_popular_id.csv", "r") as r:
            r.readline()
            for line in r.readlines():
                popular_filmarks_id.append(line.rstrip())

        popular_filmarks_id = set(popular_filmarks_id)

        mapping_table = pd.read_csv("filmarks_sid_mapping.csv")  # two cols: filmarks_id, sakuhin_public_code
        filmarks_set = set(mapping_table['filmarks_id'])
        mapping_dict = {k:v for k, v in zip(mapping_table['filmarks_id'], mapping_table['sakuhin_public_code'])}

        nb_filmarks_sakuhins = len(self.model.user_item_matrix.id2item)

        with open(self.out_path, "w") as w:
            for i in range(len(self.model.user_item_matrix.id2item)):
                filmarks_id = self.model.user_item_matrix.id2item[i]
                if filmarks_id not in filmarks_set and filmarks_id in popular_filmarks_id:
                    similar_items = self.model.bpr_model.similar_items(i, nb_filmarks_sakuhins)
                    similar_items = similar_items[1:]
                    post_play_list = []
                    for item_index, score in similar_items:
                        similar_sid = mapping_dict.get(self.model.user_item_matrix.id2item[item_index], None)
                        if similar_sid:
                            # deal with cases like SID0031110|SID0039567|SID0031110|SID0039567...
                            # since there are same series but different seasons
                            if "|" in similar_sid:
                                similar_sid = similar_sid.split("|")[0]
                            w.write("{},{},{:.3f},{}\n".format(
                                filmarks_id, similar_sid, score, pd.datetime.now().strftime("%Y-%m-%d")))
                            post_play_list.append(self.model.user_item_matrix.id2item[item_index])
                            if len(post_play_list) > top_k:
                                break

                    counter += 1
                    if counter >= nb_kensaku:
                        logging.info("got enough sakuhins for kensaku")
                        break

                    if not self.quiet and i % 100 == 0:
                        logging.info("{}".format(counter))


if __name__ == '__main__':

    DO_TRAINING = 0
    DO_RECO = 1

    if DO_TRAINING:
        clean_opts = {
            "ratings_path": "/home/ubuntu/projects/ds-offline_metrics/data/bpr_training.csv",
            "v_xiao": False,
            "out_path": "clean_ratings.csv",
            "min_sakuhin": 5,
        }
        Cleaner(clean_opts).clean()

        opts = {
            "ratings": "clean_ratings.csv",
            "algo": "implicit",
            "out_path": "/home/ubuntu/projects/ds-offline_metrics/data/bpr_training.model",
            # a list of all user id
            "supported_users_list": "/home/ubuntu/projects/ds-offline_metrics/data/supported_users_list.csv",
            # a list of all item id
            "supported_items_list": "/home/ubuntu/projects/ds-offline_metrics/data/supported_items_list.csv",
            "quiet": "True"
        }
        Trainer(opts).train()
    elif DO_RECO:
        pass
