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

    def rank(self, target_users, target_items, N=200, filter_already_liked_items=True, batch_size=10000):
        """
        :param target_users: UID list, [uid1, uid2, ...]; None means all users in matrix
        :param target_items: SID list  [sid1, sid2, ...]; None means all items in matrix
        :param N: minimum nb of output
        :yield: user_id, reco list [SID1, SID2, ...]
        """
        # filtering out users who are not in matrix
        filtered_users = self.collab_model.filter_users(target_users)
        filtered_users_ids = list(map(lambda x: self.collab_model.model.user_item_matrix.user2id[x], filtered_users))

        # filtering out items who are not in matrix
        filtered_items = self.collab_model.filter_items(target_items)
        filtered_items_ids = list(map(lambda x: self.collab_model.model.user_item_matrix.item2id[x], filtered_items))
        item_vector = self.collab_model.model.item_factors.take(filtered_items_ids, axis=0)

        if filter_already_liked_items:
            # seen matrix
            filtered_seen_matrix = self.collab_model.model.user_item_matrix.matrix[:, filtered_items_ids]

            for sub_user_list in batch(filtered_users_ids, batch_size):
                sub_users = self.collab_model.model.user_factors.take(sub_user_list, axis=0)
                subset_users_seen_items = filtered_seen_matrix[sub_user_list, :].todense()

                ordered_indexes, first_item_seen_index = reranker_x(sub_users, item_vector, filtered_items_ids,
                                                                    subset_users_seen_items, filter_seen=True)

                for user_index, reco in zip(sub_user_list, self.collab_model.defactorize_sids(ordered_indexes)):
                    yield self.collab_model.model.user_item_matrix.id2user[user_index], list(reco)[:N]
        else:
            for sub_user_list in batch(filtered_users_ids, batch_size):
                sub_users = self.collab_model.model.user_factors.take(sub_user_list, axis=0)
                ordered_indexes, _ = reranker_x(sub_users, item_vector, filtered_items_ids)
                for user_index, reco in zip(sub_user_list, self.collab_model.defactorize_sids(ordered_indexes)):
                    yield self.collab_model.model.user_item_matrix.id2user[user_index], list(reco)[:N]
