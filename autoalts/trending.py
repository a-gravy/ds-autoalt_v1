import logging
from autoalts.autoalt_maker import AutoAltMaker
from autoalts.utils import efficient_reading
from ranker import Ranker

logging.basicConfig(level=logging.INFO)


class Trending(AutoAltMaker):
    def __init__(self, **kwargs):
        super().__init__(kwargs["alt_info"], kwargs["create_date"], kwargs["blacklist_path"], kwargs["series_path"],
                         kwargs["record_path"], kwargs["max_nb_reco"], kwargs["min_nb_reco"])
        self.target_users = None
        if kwargs['target_users_path']:
            self.target_users = self.read_target_users(kwargs['target_users_path'])

        self.batch_size = int(kwargs["batch_size"])

    def make_alt(self, **kwargs):
        logging.info(f"making {self.alt_info} using model:{kwargs['model_path']}")
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.ippan_sakuhin(kwargs['pool_path'], kwargs['model_path'])
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")



    def ippan_sakuhin(self, trending_path, model_path):

        ranker = Ranker(model_path=model_path)

        pool_SIDs = set()
        # "episode_code,SID,display_name,main_genre_code,nb_watch"
        for line in efficient_reading(trending_path, True, "today_top_rank,sakuhin_public_code,sakuhin_name,uu,zenkai_uu,trend_perc"):
            arr = line.rstrip().split(",")
            if float(arr[-1]) < 1.0:
                break
            pool_SIDs.add(line.split(",")[1])

        logging.info(f"nb of SID in pool = {len(pool_SIDs)}")
        pool_SIDs = self.rm_series(pool_SIDs)
        logging.info(f"nb of SID in pool after removal same series = {len(pool_SIDs)}")

        model = self.load_model(model_path)

        nb_all_users = 0
        nb_output_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
            w.write(self.config['header']['feature_table'])
            #for userid, sid_list, score_list in rerank(model, target_users=self.target_users, target_items=pool_SIDs, filter_already_liked_items=True, batch_size=10000):
            for userid, sid_list in ranker.rank(target_users=self.target_users, target_items=pool_SIDs,
                                                filter_already_liked_items=True, batch_size=self.batch_size):
                nb_all_users += 1
                if nb_all_users % 50000 == 0:
                    logging.info(
                        'progress: {:.3f}%'.format(float(nb_all_users) / len(model.user_item_matrix.user2id) * 100))

                # remove blacklist & sids got reco already
                reco = self.remove_black_duplicates(userid, sid_list)

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
        self.reco_record.output_record()

