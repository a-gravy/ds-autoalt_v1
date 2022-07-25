"""
basic alt is
Autoalts: pool -> ranked by score
no other special operations

e.g. popular & trending
"""
import os
import logging
from autoalts.autoalt_maker import AutoAltMaker
from autoalts.utils import efficient_reading
from ranker import Ranker
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)


class BasicALT(AutoAltMaker):
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
            self.reco_record.close()
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def read_pool(self, pool_path):
        raise "Umplemenatation Error"

    def ippan_sakuhin(self, pool_path, model_path):
        ranker = Ranker(model_path=model_path)
        pool_SIDs = self.read_pool(pool_path)

        logging.info(f"nb of SID in pool = {len(pool_SIDs)}")
        pool_SIDs = self.rm_series(pool_SIDs)
        logging.info(f"nb of SID in pool after removal same series = {len(pool_SIDs)}")

        model = self.load_model(model_path)

        nb_all_users = 0
        nb_output_users = 0

        if not os.path.exists(f"{self.alt_info['feature_public_code'].values[0]}.csv"):  # if this is the first one writing this file, then output header
            logging.info("first one to create file, write header")
            with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
                w.write(self.config['header']['feature_table'])
        else:
            logging.info(f"{self.alt_info['feature_public_code'].values[0]}.csv exists")

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
            #for userid, sid_list, score_list in rerank(model, target_users=self.target_users, target_items=pool_SIDs, filter_already_liked_items=True, batch_size=10000):
            for userid, sid_list in tqdm(ranker.rank(target_users=self.target_users, target_items=pool_SIDs,
                                                filter_already_liked_items=True, batch_size=self.batch_size), miniters=50000):
                nb_all_users += 1

                # remove blacklist & sids got reco already
                reco = self.remove_black_duplicates(userid, sid_list)

                if len(reco) < self.min_nb_reco:
                    continue
                else:
                    # update reco_record
                    self.reco_record.update_record(userid, sids=reco, all=False)

                w.write(self.output_reco(userid, reco))
                nb_output_users += 1

        logging.info(
            "{} users got reco / total nb of user: {}, coverage rate={:.3f}%".format(nb_output_users, nb_all_users,
                                                                                     nb_output_users / nb_all_users * 100))


