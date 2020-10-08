"""
3 logics to make new_arrival ALT

1). new EPs
* POC@new_arrival-new_ep.ipynb
* new_ep_recommender()

2). new SIDs
two approaches
* based on User Similarity
* based on Tag Similarity

3). popular new SIDs -> different implementation, future project


=> mix 1, 2, 3 together


"""

import pandas as pd
import pickle
import os, sys, tempfile, logging, time
import numpy as np
import time
import operator
#from implicit.bpr import BayesianPersonalizedRanking
#from recoalgos.matrixfactor.bpr import BPRRecommender
#from scipy.sparse import coo_matrix


logging.basicConfig(level=logging.INFO)

# TODO: move to tool.py
def load_model(path):
    return pickle.load(open(path, "rb"))

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def rerank_seen(model,
                target_users=None,
                target_items=None,
                batch_size=500):
    """
    rank seens SIDs and return

    :param target_users: UID list, [uid1, uid2, ...]; None means all users in matrix
    :param target_items: SID list  [sid1, sid2, ...]; None means all items in matrix
    :param filter_already_liked_items: removed the nonzeros item in matrix, set False if you have special filtering
    :param N: minimum nb of output
    :param batch_size:
    :yield: uid, sid list
    """

    # user id -> matrix index
    if target_users is None:
        target_users_index_list = list(model.user_item_matrix.user2id.values())  # TODO [:10] is for testing
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
            nonzeros_in_row = set(rows[i].nonzero()[1])
            best = sorted(enumerate(scores[i]), key=lambda x: -x[1])

            # only keep nonzero indice == already seen items
            reranked_targer_items = [(index, score) for index, score in best if index in nonzeros_in_row]
            score_list = [score for index, score in reranked_targer_items]
            # target matrix index -> matrix index -> item id
            reranked_item_indexs = [model.user_item_matrix.id2item[target_items2actualid[index]]
                                    for index, score in reranked_targer_items]

            yield model.user_item_matrix.id2user[uid], reranked_item_indexs, score_list


def new_arrival_ep_loader(input_path="data/new_arrival_EP.csv"):
    """
    :return: dict {sakuhin_public_code:episode_public_code}
    """
    eps = pd.read_csv(input_path)
    # dataframe already ordered by episode_no -> keep the most front ep
    eps = eps[~ eps.duplicated(subset='sakuhin_public_code', keep='first')]
    return {sid: epc for sid, epc in zip(eps['sakuhin_public_code'], eps['episode_public_code'])}


# past N days is corresponding to past N days of new_arrival_EP.csv
def user_session_reader(input_path="data/new_user_sessions.csv"):
    """
    input format:
    "user_id", "SIDs..." , "episode_public_codes...", "watch times"

    :return: a dict {user_id: { SID: episode_public_codes}}
    """
    us_dict = {}
    with open(input_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().replace('"', '').split(",")
                nb = len(arr) - 1
                SIDs = arr[1:1 + int(nb / 3)]
                EPs = arr[1 + int(nb / 3):1 + int(nb / 3) * 2]
                us_dict.setdefault(arr[0], {k:v for k, v in zip(SIDs, EPs)})
            else:
                break
    return us_dict


def new_ep_recommender_bk(model_path):
    """
    669933 users are binge-watching sth,
    took 494s

    :return: a dict {user_id:{SIDA:scoreA, SIDB:scoreB, ...}
    """
    model = load_model(model_path)
    new_arrival_sid_epc = new_arrival_ep_loader()
    user_session = user_session_reader()

    logging.info(f"{len(new_arrival_sid_epc)} SIDs w/ new arrival EP")

    # new ep is high priority, score = bpr_score + score_base
    score_base = 100

    new_ep_reco = {}
    for i, (uid, sid_list, score_list) in enumerate(rerank_seen(model, None,
                                                                target_items=list(new_arrival_sid_epc.keys()),
                                                                batch_size=10000)):
        if i%10000 == 0:
            logging.info(f'progress: {float(i)/len(model.user_item_matrix.user2id)}')
        # get new EPs of user binge-watching sakuhins

        user_interesting_new_eps = [(sid, new_arrival_sid_epc.get(sid)) for sid in sid_list]
                                    # if new_arrival_sid_epc.get(sid, False)]

        if user_interesting_new_eps:
            watched_eps = user_session.get(uid, None)
            if watched_eps:  # remove already watched EP
                watched_eps = set(watched_eps)
                new_ep_reco.setdefault(uid,
                                       {sid:score.item() for (sid, ep), score in zip(user_interesting_new_eps, score_list)
                                        if ep not in watched_eps})
                # new_ep_reco.setdefault(uid, {'SIDs':[sid for sid, ep in user_interesting_new_eps if ep not in watched_eps]})
            else:
                new_ep_reco.setdefault(uid, {sid:score.item()+score_base for sid, score in zip(sid_list, score_list)})
    logging.info(f"{len(new_ep_reco)} users are binge-watching sth")
    # TODO: same series reco

    # TODO: update N past days daily or just one day daily?

    return new_ep_reco


def new_ep_recommender(alt_info, create_date, model_path, min_nb_reco):
    """
    669933 users are binge-watching sth,
    took 494s

    :return: a dict {user_id:{SIDA:scoreA, SIDB:scoreB, ...}
    """
    model = load_model(model_path)
    new_arrival_sid_epc = new_arrival_ep_loader()
    user_session = user_session_reader()

    logging.info(f"{len(new_arrival_sid_epc)} SIDs w/ new arrival EP")

    with open(f"{alt_info['feature_public_code'].values[0]}.csv", "w") as w:  # TODO: new_arrival_reco
        w.write("user_multi_account_id,feature_public_code,create_date,sakuhin_codes,"
                "feature_name,feature_description,domain,is_autoalt\n")
        for i, (userid, sid_list, score_list) in enumerate(rerank_seen(model, None,
                                                                    target_items=list(new_arrival_sid_epc.keys()),
                                                                    batch_size=10000)):
            if i%10000 == 0:
                logging.info('progress: {:.3f}'.format(float(i)/len(model.user_item_matrix.user2id)*100))

            # get new EPs of user binge-watching sakuhins
            user_interesting_new_eps = [(sid, new_arrival_sid_epc.get(sid)) for sid in sid_list]

            if len(user_interesting_new_eps) < min_nb_reco:
                continue

            if user_interesting_new_eps:
                watched_eps = user_session.get(userid, None)
                if watched_eps:  # remove already watched EP
                    watched_eps = set(watched_eps)

                    unseen_new_eps_sids = [sid for (sid, ep) in user_interesting_new_eps
                                            if ep not in watched_eps]

                    if len(unseen_new_eps_sids) < min_nb_reco:
                        continue

                    w.write(f"{userid},{alt_info['feature_public_code'].values[0]},{create_date},{'|'.join(unseen_new_eps_sids)},"
                            f"{alt_info['feature_name'].values[0]},,{alt_info['domain'].values[0]},1\n")

                    #new_ep_reco.setdefault(userid,
                    #                       {sid:score.item() for (sid, ep), score in zip(user_interesting_new_eps, score_list)
                    #                        if ep not in watched_eps})
                else:
                    w.write(f"{userid},{alt_info['feature_public_code'].values[0]},{create_date},{'|'.join(sid_list)},"
                            f"{alt_info['feature_name'].values[0]},,{alt_info['domain'].values[0]},1\n")
                    # new_ep_reco.setdefault(userid, {sid:score.item()+score_base for sid, score in zip(sid_list, score_list)})
    # TODO: same series reco

    # TODO: update N past days daily or just one day daily?


def reco_by_user_similarity(model_path,
                            nb_similar_user=30000,
                            new_arrival_SIDs_path="data/new_arrival_SID.csv",
                            new_user_session_path="data/new_user_sessions.csv"):
    """
    nb_similar_user=10000
    142 new arrival SIDs -> 110 done / 32 sakuhins haven't been watched yet
    user coverage rate: 1767708/1900365 = 0.93
    took 185s ~= 6m
    ========
    nb_similar_user=30000
    user coverage rate: 1947403/1947410 = 0.9999964054821532
    143 done / 21 sakuhins haven't been watched yet
    execution time = 4207.13515496254
    ========
    nb_similar_user=100000
    142 new arrival SIDs -> 110 done / 32 sakuhins haven't been watched yet
    user coverage rate: 1900222/1900365 = 0.9999
    took 1033.5094525814056

    :return: new_sid_reco {'PM023203146': {'SIDs': ['SID0048896',...], 'scores': [2.183024, ...]} }
    """
    logging.info("running reco_by_user_similarity")

    model = load_model(model_path)

    id2user = {}
    for user, id_ in model.user_item_matrix.user2id.items():
        id2user[id_] = user

    # read new arrival SIDs as dict( SID: [])
    new_arrival_SIDs = {}
    with open(new_arrival_SIDs_path, "r") as r:
        r.readline()
        for line in r.readlines():
            new_arrival_SIDs.setdefault(line.rstrip().replace('"', ''), [])
    logging.info(f"{len(new_arrival_SIDs)} new arrival SIDs ")

    # user sessions for new arrival SIDs
    line_counter = 0
    with open(new_user_session_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                if line_counter%10000 == 1:
                    logging.info(f"{line_counter} done")
                line_counter+=1

                arr = line.rstrip().replace('"', '').split(",")
                nb = len(arr) - 1
                SIDs = arr[1:1 + int(nb / 3)]
                for SID in SIDs:
                    user_id_list = new_arrival_SIDs.get(SID, None)
                    if user_id_list != None and arr[0] not in user_id_list:
                        new_arrival_SIDs[SID] = user_id_list + [arr[0]]
            else:
                break

    # make reco
    new_arrival_reco = {}
    nb_nobody_wacthed_sids = []
    for SID, watched_user_list in new_arrival_SIDs.items():
        if not watched_user_list:
            nb_nobody_wacthed_sids.append(SID)
            continue

        similar_users = {}  # user_index: similarity score
        for user_id in watched_user_list:
            u_index = model.user_item_matrix.user2id.get(user_id, None)
            if u_index:
                # TODO: use the largest socre
                similar_users.update({index: score for (index, score) in model.bpr_model.similar_users(u_index, nb_similar_user)})

        # 186132: 2.8023417  ->  PM017329518:{ SID0048732:2.8023417 }
        for user_index, score in similar_users.items():
            user_id = id2user[user_index]
            if user_id in new_arrival_reco:
                new_arrival_reco[user_id].update({SID:score})
            else:
                new_arrival_reco.setdefault(user_id, {SID:score})

    logging.info(f" {len(new_arrival_SIDs) - len(nb_nobody_wacthed_sids)} done / {len(nb_nobody_wacthed_sids)} sakuhins haven't been watched yet")
    logging.info(f"user coverage rate: {len(new_arrival_reco)}/{len(model.user_item_matrix.user2id)} = "
                 f"{float(len(new_arrival_reco))/len(model.user_item_matrix.user2id)}")

    return new_arrival_reco, nb_nobody_wacthed_sids


def video_domain(alt_info, create_date, model_path, output_name):
    start_time = time.time()

    # new ep -> high priority -> score = 100 + bpr score
    new_arrival_ep_reco = new_ep_recommender(model_path)
    logging.info(f"took {time.time() - start_time}")

    logging.info("merge & rank by score")
    with open(output_name, "w") as w:  # TODO: new_arrival_reco
        w.write("user_multi_account_id,feature_public_code,create_date,sakuhin_codes,"
                "feature_name,feature_description,domain,is_autoalt\n")
        for userid in new_arrival_ep_reco.keys():
            # combine recommendations
            sid_score_dict = {}
            sid_score_dict.update(new_arrival_ep_reco.get(userid, {}))
            sid_list, score_list = [], []
            for k, v in sorted(sid_score_dict.items(), key=operator.itemgetter(1), reverse=True):
                sid_list.append(k)
                score_list.append('{:.3f}'.format(v))
            # w.write(f'{user_id},{"|".join(sid_list)},1.0\n')
            w.write(f"{userid},{alt_info['feature_public_code'].values[0]},{create_date},{'|'.join(sid_list)},"
                    f"{alt_info['feature_name'].values[0]},,{alt_info['domain'].values[0]},1\n")

    """
    # user-similarity  TODO: replaced by serendipity
    start_time = time.time()
    new_arrival_sid_reco, nb_nobody_wacthed_sids = reco_by_user_similarity(model_path)
    logging.info(f"took {time.time() - start_time}")

    logging.info("merge & rank by score")
    with open(output_name, "w") as w:  # TODO: new_arrival_reco
        for user_id in (set(new_arrival_ep_reco.keys()) | set(new_arrival_sid_reco.keys())):
            # combine recommendations
            sid_score_dict = {}
            sid_score_dict.update(new_arrival_ep_reco.get(user_id, {}))
            sid_score_dict.update(new_arrival_sid_reco.get(user_id, {}))
            sid_list, score_list = [], []
            for k,v in sorted(sid_score_dict.items(), key=operator.itemgetter(1), reverse=True):
                sid_list.append(k)
                score_list.append('{:.3f}'.format(v))
            w.write(f'{user_id},{"|".join(sid_list)},1.0\n')
    """


def make_alt(alt_info, create_date, model_path, min_nb_reco):
    logging.info(f"making {alt_info} using model:{model_path}")
    if alt_info['domain'].values[0] == "video":
        new_ep_recommender(alt_info, create_date, model_path, min_nb_reco)
    elif alt_info['domain'].values[0] == "book":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")


if __name__ == '__main__':
    pass


































