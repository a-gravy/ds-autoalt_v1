import logging
import pandas as pd
import operator
from autoalts.autoalt_maker import AutoAltMaker
from autoalts.utils import efficient_reading
from ranker import Ranker
import datetime
import random

logging.basicConfig(level=logging.INFO)


class Exclusives(AutoAltMaker):
    def __init__(self,**kwargs):
        super().__init__(kwargs["alt_info"], kwargs["create_date"], kwargs["blacklist_path"], kwargs["series_path"],
                         kwargs["record_path"], kwargs["max_nb_reco"], kwargs["min_nb_reco"])
        self.target_users = None
        if kwargs['target_users_path']:
            self.target_users = self.read_target_users(kwargs['target_users_path'])

        self.batch_size = int(kwargs["batch_size"])

    def make_alt(self, **kwargs):
        logging.info(f"making {self.alt_info} using model:{kwargs['model_path']}")
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.make_coldstart(kwargs['pool_path'], kwargs['MAINPAGE_top_alts_path'])
            self.ippan_sakuhin_mixing_new_arrivals(kwargs['pool_path'], kwargs['model_path'])
            self.reco_record.close()
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def ippan_sakuhin(self, pool_path, model_path):

        ranker = Ranker(model_path=model_path)

        pool_SIDs = set()
        for line in efficient_reading(pool_path, True):
            pool_SIDs.add(line.split(",")[0].replace('"', ''))

        logging.info(f"nb of SID in pool = {len(pool_SIDs)}")
        pool_SIDs = self.rm_series(pool_SIDs)
        logging.info(f"nb of SID in pool after removal same series = {len(pool_SIDs)}")

        model = self.load_model(model_path)

        nb_all_users = 0
        nb_output_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
            w.write(self.config['header']['feature_table'])
            for userid, sid_list in ranker.rank(target_users=self.target_users, target_items=pool_SIDs,
                                                filter_already_liked_items=True, batch_size=self.batch_size):
                nb_all_users += 1
                if nb_all_users % 50000 == 0:
                    logging.info(
                        'progress: {:.3f}%'.format(float(nb_all_users) / len(model.user_item_matrix.user2id) * 100))

                # remove blacklist
                rm_sids = self.blacklist
                reco = [SID for SID in sid_list if SID not in rm_sids]

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

    def get_new_arrivals(self, pool_path):
        """
        extract New Arrivals 独占 SIDs within new_arrival_date(one week)

        :param pool_path:
        :return: SID list order by POPULARITY_POINT
        """
        df = pd.read_csv(pool_path)
        new_arrival_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        new_arrivals = df[df['exclusive_start_datetime'] >= new_arrival_date]
        new_arrivals = new_arrivals.sort_values(['POPULARITY_POINT'], ascending=False)
        logging.info(f"got {len(new_arrivals)} New Arrivals 独占 : {list(new_arrivals['display_name'])}  {list(new_arrivals['sakuhin_public_code'])}")

        return list(new_arrivals['sakuhin_public_code'])

    def ippan_sakuhin_mixing_new_arrivals(self, pool_path, model_path):
        # get New Arrivals
        new_arrivals = self.get_new_arrivals(pool_path)
        random.shuffle(new_arrivals)

        ranker = Ranker(model_path=model_path)
        pool_SIDs = set()
        for line in efficient_reading(pool_path, True):
            pool_SIDs.add(line.split(",")[0].replace('"', ''))

        logging.info(f"nb of SID in pool = {len(pool_SIDs)}")
        pool_SIDs = self.rm_series(pool_SIDs)
        logging.info(f"nb of SID in pool after removal same series = {len(pool_SIDs)}")

        # make personalized ALT for every user
        model = self.load_model(model_path)
        nb_all_users = 0
        nb_output_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
            w.write(self.config['header']['feature_table'])
            for userid, sid_list in ranker.rank(target_users=self.target_users, target_items=pool_SIDs,
                                                filter_already_liked_items=True, batch_size=self.batch_size):
                nb_all_users += 1
                if nb_all_users % 50000 == 0:
                    logging.info(
                        'progress: {:.3f}%'.format(float(nb_all_users) / len(model.user_item_matrix.user2id) * 100))

                # remove blacklist & sids got reco already
                reco = self.remove_black_duplicates(userid, sid_list)[:self.max_nb_reco]
                new_arrivals_r = [x for x in new_arrivals if x not in reco]

                # random calculate positions for new arrival
                if new_arrivals_r:
                    random_position = [1]  # start from 1
                    for i in range(len(new_arrivals_r)-1):
                        random_position.append(random_position[-1] + random.randint(2, 3))

                    for i, pos in enumerate(random_position):
                        reco.insert(pos, new_arrivals_r[i])
                else:
                    logging.info(f"{userid} got 0 new arrivals")

                if len(reco) < self.min_nb_reco:
                    continue
                else:
                    # update reco_record
                    self.reco_record.update_record(userid, sids=reco, all=False)

                if not self.check_inline_duplicates(reco):
                    logging.info(f"duplicates in {userid} {reco}")
                    raise ""

                w.write(self.output_reco(userid, reco))
                nb_output_users += 1

            logging.info(
                "{} users got reco / total nb of user: {}, coverage rate={:.3f}%".format(nb_output_users, nb_all_users,
                                                                                         nb_output_users / nb_all_users * 100))


    def make_coldstart(self, pool_path, MAINPAGE_top_alts_path):
        # TODO: also remove duplicates in daily top
        df = pd.read_csv(pool_path)

        top_alt_sids = set()
        for line in efficient_reading(MAINPAGE_top_alts_path, True, "feature_public_code,sakuhin_public_code"):
            top_alt_sids.add(line.rstrip().split(",")[1])

        coldstart = [x for x in df.sort_values(['POPULARITY_POINT'], ascending=False)['sakuhin_public_code'] if x not in top_alt_sids]
        coldstart = self.rm_series(coldstart)
        print("coldstart SIDs = ")
        print(",".join(coldstart))
