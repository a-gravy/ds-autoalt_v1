import logging
import operator
from autoalts.autoalt_maker import AutoAltMaker
from utils import efficient_reading
from bpr.implicit_recommender import rerank

logging.basicConfig(level=logging.INFO)


class Popular(AutoAltMaker):
    def __init__(self, alt_info, create_date, target_users_path, blacklist_path, series_path=None, max_nb_reco=20, min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)
        self.target_users = None
        if target_users_path:
            self.target_users = self.read_target_users(target_users_path)

    def make_alt(self, popular_sids_path, already_reco_path, bpr_model_path):
        logging.info(f"making {self.alt_info} using model:{bpr_model_path}")
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.ippan_sakuhin(popular_sids_path, already_reco_path, bpr_model_path)
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def ippan_sakuhin(self, popular_sids_path, already_reco_path, bpr_model_path):
        pool_SIDs = set()
        for line in efficient_reading(popular_sids_path, True, "main_genre_code,sakuhin_public_code,sakuhin_name,row_th"):
            pool_SIDs.add(line.split(",")[1])

        logging.info(f"nb of SID in pool = {len(pool_SIDs)}")
        pool_SIDs = self.rm_series(pool_SIDs)
        logging.info(f"nb of SID in pool after removal same series = {len(pool_SIDs)}")

        already_reco = {}  # userid, SID|SID|...
        for line in efficient_reading(already_reco_path, True, "userid,sid_list"):
            arr = line.rstrip().split(",")
            already_reco[arr[0]] = arr[1]

        model = self.load_model(bpr_model_path)

        nb_all_users = 0
        nb_output_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for userid, sid_list, score_list in rerank(model, target_users=self.target_users, target_items=pool_SIDs,
                                                       filter_already_liked_items=True, batch_size=10000):
                nb_all_users += 1
                if nb_all_users % 50000 == 0:
                    logging.info(
                        'progress: {:.3f}%'.format(float(nb_all_users) / len(model.user_item_matrix.user2id) * 100))

                rm_sids = already_reco.get(userid, None)
                if rm_sids:
                    rm_sids = set(rm_sids.split('|'))
                    sid_list = [sid for sid in sid_list if sid not in rm_sids]

                reco = self.black_list_filtering(sid_list)
                # reco = self.rm_duplicates(reco)

                if len(reco) < self.min_nb_reco:
                    continue

                w.write(
                    f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco[:self.max_nb_reco])},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")
                nb_output_users += 1

            logging.info(
                "{} users got reco / total nb of user: {}, coverage rate={:.3f}%".format(nb_output_users, nb_all_users,
                                                                                         nb_output_users / nb_all_users * 100))

