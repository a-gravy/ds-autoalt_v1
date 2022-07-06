import logging
from autoalts.autoalt_maker import AutoAltMaker
from utils import efficient_reading
from bpr.implicit_recommender import rerank

logging.basicConfig(level=logging.INFO)


class Trending(AutoAltMaker):
    def __init__(self, alt_info, create_date, target_users_path, blacklist_path, series_path=None, max_nb_reco=30, min_nb_reco=2):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)
        self.target_users = None
        if target_users_path:
            self.target_users = self.read_target_users(target_users_path)

    def make_alt(self, raw_path_top, dailytop_path, toppick_path, bpr_model_path):
        logging.info(f"making {self.alt_info} using model:{bpr_model_path}")
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.ippan_sakuhin(raw_path_top, dailytop_path, toppick_path, bpr_model_path)
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def ippan_sakuhin(self, raw_path_top, dailytop_path, toppick_path, bpr_model_path):
        pool_SIDs = set()
        for line in efficient_reading(raw_path_top, True, "episode_code,SID,display_name,main_genre_code,nb_watch"):
            pool_SIDs.add(line.split(",")[1])

        logging.info(f"nb of SID in pool = {len(pool_SIDs)}")

        for line in efficient_reading(dailytop_path, True,
                                      "user_multi_account_id,feature_public_code,create_date,sakuhin_codes,feature_title,domain,autoalt,feature_public_start_datetime,feature_public_end_datetime"):
            daily_top_SIDs = line.split(",")[3].split("|")

        toppick_dict = {}  # user:list(SID)
        for line in efficient_reading(toppick_path, True, "user_multi_account_id,platform,block,sakuhin_codes,feature_name,sub_text,score,create_date"):
            arr = line.split(",")
            # Only remove top 5 SIDs, since user may only see the top 5
            toppick_dict[arr[0]] = arr[3].split("|")[:5]

        pool_SIDs = pool_SIDs - set(daily_top_SIDs)
        logging.info(f"nb of SID in pool after removal daily_top = {len(pool_SIDs)}")
        pool_SIDs = self.rm_series(pool_SIDs)
        logging.info(f"nb of SID in pool after removal same series = {len(pool_SIDs)}")

        model = self.load_model(bpr_model_path)

        nb_all_users = 0
        nb_output_users = 0

        # for popular, record the
        already_reco_output = open("already_reco_for_popular.csv", "w")
        already_reco_output.write("userid,sid_list\n")

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for userid, sid_list, score_list in rerank(model, target_users=self.target_users, target_items=pool_SIDs,
                                                       filter_already_liked_items=True,
                                                       batch_size=10000):

                nb_all_users += 1
                if nb_all_users % 50000 == 0:
                    logging.info(
                        'progress: {:.3f}%'.format(float(nb_all_users) / len(model.user_item_matrix.user2id) * 100))

                toppick_sids = toppick_dict.get(userid, None)
                if toppick_sids:
                    already_reco_output.write(f"{userid},{'|'.join(sid_list + toppick_sids)}\n")

                    toppick_sids = set(toppick_sids)
                    sid_list = [sid for sid in sid_list if sid not in toppick_sids]
                else:
                    already_reco_output.write(f"{userid},{'|'.join(sid_list)}\n")

                reco = self.black_list_filtering(sid_list)
                reco = self.rm_duplicates(reco)

                if len(reco) < self.min_nb_reco:
                    continue

                w.write(
                    f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco)},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")
                nb_output_users += 1

        logging.info(
            "{} users got reco / total nb of user: {}, coverage rate={:.3f}%".format(nb_output_users, nb_all_users,
                                                                                     nb_output_users / nb_all_users * 100))
        already_reco_output.close()



