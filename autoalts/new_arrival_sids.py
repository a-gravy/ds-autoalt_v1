import logging
from autoalts.autoalt_maker import AutoAltMaker
from autoalts.utils import efficient_reading
import datetime
import numpy as np
from tqdm import tqdm
import random
import copy

logging.basicConfig(level=logging.INFO)


class NewArrivalSIDs(AutoAltMaker):
    def __init__(self,**kwargs):
        super().__init__(kwargs["alt_info"], kwargs["create_date"], kwargs["blacklist_path"], kwargs["series_path"],
                         kwargs["record_path"], kwargs["max_nb_reco"], kwargs["min_nb_reco"])
        self.target_users = set()
        if kwargs['target_users_path']:
            self.target_users = self.read_target_users(kwargs['target_users_path'])

        self.batch_size = int(kwargs["batch_size"])
        self.pbar = kwargs["pbar"]

        # TODO
        if kwargs['watched_list_path']:
            logging.info("adding watched SIDs into reco")
            for line in efficient_reading(kwargs['watched_list_path'], with_header=False):
                arr = line.rstrip().split(",")  # user_multi_account_id,sids
                self.reco_record.update_record(user_id=arr[0], sids=arr[1].split("|"), all=True)

    def make_alt(self, **kwargs):
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.ippan_sakuhin(kwargs['pool_path'], kwargs['user_profiling_path'])
            self.reco_record.close()
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    @staticmethod
    def read_pool(pool_path):
        na_genre_mapping = dict()  # {genre: set(SIDs)}
        all_na_sids = []  # list of all new arrival SIDs in order of uu desc
        # TODO: check header & format
        for line in efficient_reading(pool_path,
                                      header_format="sakuhin_public_code,sakuhin_name,main_genre_code,first_sale_datetime,production_year,uu"):
            arr = line.rstrip().split(",")
            na_genre_mapping.setdefault(arr[2], {arr[0]}).update({arr[0]})
            if arr[-1]:
                all_na_sids.append(arr[0])

        assert len(na_genre_mapping) > 0, "Error, na_genre_mapping is empty"

        na_genre_mapping = {k:list(v) for k, v in na_genre_mapping.items()}  # convert set to list

        for k, v in na_genre_mapping.items():
            logging.info(f"new arrivals: {len(v)} SIDs in genre:{k}")

        return na_genre_mapping, all_na_sids, set(all_na_sids)

    def ippan_sakuhin(self, pool_path, user_profiling_path):
        na_genre_mapping, all_na_sids, all_na_sids_set = self.read_pool(pool_path)

        # TODO: change
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            pbar = tqdm(efficient_reading(user_profiling_path, with_header=False)) \
                if self.pbar else efficient_reading(user_profiling_path, with_header=False)
            for line in pbar:
                arr = line.rstrip().split(",")
                userid = arr[0]
                if self.target_users and userid not in self.target_users:
                    continue

                # shuffle the pool for every user
                na_genre_mapping_pool = copy.deepcopy(na_genre_mapping)
                for genre, sids in na_genre_mapping.items():
                    random.shuffle(na_genre_mapping_pool[genre])

                # make positions of genre using preference ratio
                arr = line.split(",")
                profile = {}  # genre:playtime
                total_time = 0
                for genre, playt_time in zip(arr[2].split("|"), arr[3].split("|")):
                    profile[genre] = profile.setdefault(genre, 0) + int(playt_time)
                    total_time += int(playt_time)

                preference = list(profile.keys())
                prob = [x/total_time for x in profile.values()]  # [float(x)/100.0 for x in arr[2].split("|")[:-1]]
                prob[-1] = (1.0 - sum(prob[:-1]))
                genre_order = np.random.choice(preference, size=50, replace=True, p=prob)

                reco = []
                for genre in genre_order:
                    if genre in na_genre_mapping_pool and len(na_genre_mapping_pool[genre]) != 0:
                        reco.append(na_genre_mapping_pool[genre].pop())
                        if len(reco) >= self.max_nb_reco:
                            break

                if len(reco) < self.max_nb_reco:
                    reco = reco + [sid for sid in all_na_sids if sid not in reco]
                    # reco += list(np.random.choice(list(all_na_sids - set(reco)), size=self.max_nb_reco-len(reco)+10, replace=False))
                reco = reco[:self.max_nb_reco + 10]
                reco = self.remove_black_duplicates(userid, reco)
                self.reco_record.update_record(userid, sids=reco[:self.max_nb_reco])

                w.write(
                    f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco[:self.max_nb_reco])},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

