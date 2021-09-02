import logging
import numpy as np
import pandas as pd
from dscollaborative.recommender import ImplicitModel #, Features, Cleaner, Duplicates
from dscollaborative.recommender import reranker_x
from autoalts.utils import batch

logging.basicConfig(level=logging.INFO)


class Ranker:
    def __init__(self, model_path):
        self.collab_model = ImplicitModel()
        self.collab_model.load_model(model_path)

    def rank(self, target_users, target_items, N=200, batch_size=10000):
        """
        :param target_users: UID list, [uid1, uid2, ...]; None means all users in matrix
        :param target_items: SID list  [sid1, sid2, ...]; None means all items in matrix
        :param N: minimum nb of output
        :yield: user_id, reco list [SID1, SID2, ...]

        :param filter_already_liked_items: removed the nonzeros item in matrix, set False if you have special filtering

        """
        # filtering out users who are not in matrix
        filtered_users = self.collab_model.filter_users(target_users)
        filtered_users_ids = list(map(lambda x: self.collab_model.model.user_item_matrix.user2id[x], filtered_users))

        filtered_items = self.collab_model.filter_items(target_items)
        filtered_items_ids = list(map(lambda x: self.collab_model.model.user_item_matrix.item2id[x], filtered_items))
        item_vector = self.collab_model.model.item_factors.take(filtered_items_ids, axis=0)

        for sub_user_list in batch(filtered_users_ids, batch_size):
            sub_users = self.collab_model.model.user_factors.take(sub_user_list, axis=0)
            ordered_indexes = reranker_x(sub_users, item_vector, filtered_items_ids)

            for user_index, reco in zip(sub_user_list, self.collab_model.defactorize_sids(ordered_indexes)):
                yield self.collab_model.model.user_item_matrix.id2user[user_index], list(reco)[:N]
