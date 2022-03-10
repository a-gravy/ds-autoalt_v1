import logging
import pandas as pd
import operator
from autoalts.autoalt_maker import AutoAltMaker
from autoalts.utils import efficient_reading
from ranker import Ranker
import datetime
from tqdm import tqdm
import random

logging.basicConfig(level=logging.INFO)


class NewArrivalSIDs(AutoAltMaker):
    def __init__(self,**kwargs):
        super().__init__(kwargs["alt_info"], kwargs["create_date"], kwargs["blacklist_path"], kwargs["series_path"],
                         kwargs["record_path"], kwargs["max_nb_reco"], kwargs["min_nb_reco"])
        self.target_users = None
        if kwargs['target_users_path']:
            self.target_users = self.read_target_users(kwargs['target_users_path'])

        self.batch_size = int(kwargs["batch_size"])
        self.pbar = kwargs["pbar"]

    def make_alt(self, **kwargs):
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.ippan_sakuhin(kwargs['pool_path'], kwargs['sakuhin_meta'], kwargs['toppick'])
            self.reco_record.output_record()
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    @staticmethod
    def read_pool(pool_path):
        na_genre_mapping = dict()  # {genre: set(SIDs)}
        for line in efficient_reading(pool_path, header_format="sakuhin_public_code,display_name,main_genre_code"):
            # sakuhin_public_code,display_name,main_genre_code
            arr = line.rstrip().split(",")
            na_genre_mapping.setdefault(arr[2], {arr[0]}).update({arr[0]})

        assert len(na_genre_mapping) > 0, "Error, na_genre_mapping is empty"

        for k, v in na_genre_mapping.items():
            logging.info(f"new arrivals: {len(v)} SIDs in genre:{k}")

        return na_genre_mapping

    @staticmethod
    def read_sakuhin_meta(sakuhin_meta_path):
        sakuhin_meta = dict()
        for line in efficient_reading(sakuhin_meta_path, header_format="sakuhin_public_code,display_name,main_genre_code,menu_names,parent_menu_names,nations"):
            arr = line.split(",")
            sakuhin_meta[arr[0]] = arr[2]
        return sakuhin_meta

    def ippan_sakuhin(self, pool_path, sakuhin_meta_path, toppick_path):
        na_genre_mapping = self.read_pool(pool_path)
        sakuhin_meta = self.read_sakuhin_meta(sakuhin_meta_path)
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
            w.write(self.config['header']['feature_table'])
            pbar = tqdm(efficient_reading(toppick_path, header_format="user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block")) \
                if self.pbar else efficient_reading(toppick_path, header_format="user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block")
            for line in pbar:
                arr = line.split(",")
                userid = arr[0]
                sids = arr[1].split("|")
                if self.target_users and userid not in self.target_users:
                    continue

                preference_genres = []
                for sid in sids:
                    genre = sakuhin_meta.get(sid, None)
                    if genre and genre not in preference_genres:
                        preference_genres.append(genre)
                """
                INFO:root:new arrivals: 53 SIDs in genre:VARIETY
                INFO:root:new arrivals: 14 SIDs in genre:ADRAMA
                INFO:root:new arrivals: 61 SIDs in genre:YOUGA
                INFO:root:new arrivals: 90 SIDs in genre:HOUGA
                INFO:root:new arrivals: 4 SIDs in genre:KIDS
                INFO:root:new arrivals: 4 SIDs in genre:FDRAMA
                INFO:root:new arrivals: 10 SIDs in genre:DOCUMENT
                INFO:root:new arrivals: 7 SIDs in genre:ANIME
                INFO:root:new arrivals: 9 SIDs in genre:DRAMA
                """
                reco = []
                # build reco from preference_genres in order, if nb of reco is enough then break
                # TODO: remove watched SIDs
                for genre in preference_genres:
                    pool = na_genre_mapping[genre]
                    pool = self.remove_black_duplicates_from_set(userid, pool)
                    nb_pick = len(pool) if len(pool) < self.max_nb_reco else self.max_nb_reco  # self.max_nb_reco+20 to avoid sids got removed
                    reco = reco + random.sample(list(pool), k=nb_pick)
                    if len(reco) >= self.max_nb_reco: # break is nb is enough
                        break

                if len(reco) < self.min_nb_reco:
                    continue
                else:
                    # update reco_record
                    self.reco_record.update_record(userid, sids=reco)

                w.write(
                    f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco[:self.max_nb_reco])},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

    def ippan_sakuhin_(self, pool_path, sakuhin_meta_path, toppick_path):
        na_genre_mapping = self.read_pool(pool_path)
        sakuhin_meta = self.read_sakuhin_meta(sakuhin_meta_path)
        show_pbar = True
        print(f"toppick_path = {toppick_path}")
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "a") as w:
            w.write(self.config['header']['feature_table'])
            pbar = tqdm(efficient_reading(toppick_path, header_format="user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block")) \
                if self.pbar else efficient_reading(toppick_path, header_format="user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block")
            for line in pbar:
                arr = line.split(",")
                userid = arr[0]
                sids = arr[1].split("|")
                if self.target_users and userid not in self.target_users:
                    continue

                preference_genres = []
                for sid in top_sids:
                    genre = sakuhin_meta.get(sid, None)
                    if genre and genre not in preference_genres:
                        preference_genres.append(genre)
                """
                INFO:root:new arrivals: 53 SIDs in genre:VARIETY
                INFO:root:new arrivals: 14 SIDs in genre:ADRAMA
                INFO:root:new arrivals: 61 SIDs in genre:YOUGA
                INFO:root:new arrivals: 90 SIDs in genre:HOUGA
                INFO:root:new arrivals: 4 SIDs in genre:KIDS
                INFO:root:new arrivals: 4 SIDs in genre:FDRAMA
                INFO:root:new arrivals: 10 SIDs in genre:DOCUMENT
                INFO:root:new arrivals: 7 SIDs in genre:ANIME
                INFO:root:new arrivals: 9 SIDs in genre:DRAMA
                """
                pool = set()
                reco = []
                for genre in preference_genres:
                    pool.update(na_genre_mapping[genre])
                    pool = self.remove_black_duplicates_from_set(userid, pool)
                    nb_pick = len(pool) if len(pool) < self.max_nb_reco else self.max_nb_reco  # self.max_nb_reco+20 to avoid sids got removed
                    reco = reco + random.sample(list(pool), k=nb_pick)
                    if len(reco) >= self.max_nb_reco:
                        break

                reference_top_n = 3

                while True:
                    # calc the preference
                    top_sids = sids[:reference_top_n]
                    preference_set = set([sakuhin_meta.get(sid, None) for sid in top_sids])
                    # calc the reco
                    pool = set()
                    for genre in preference_set:
                        if genre in na_genre_mapping:
                            pool.update(na_genre_mapping[genre])

                    if reference_top_n >= len(sids):
                        break
                    elif len(pool) < self.max_nb_reco+30:
                        reference_top_n += 5
                    else:
                        break

                if len(pool) < self.min_nb_reco:
                    print(f"#1 {len(pool)} < self.min_nb_reco")
                    continue

                # remove blacklist & sids got reco already
                pool = self.remove_black_duplicates_from_set(userid, pool)
                nb_pick = len(pool) if len(pool) < self.max_nb_reco else self.max_nb_reco  # self.max_nb_reco+20 to avoid sids got removed
                reco = random.sample(list(pool), k=nb_pick)

                if len(reco) < self.min_nb_reco:
                    print(f"#2 {len(reco)} < self.min_nb_reco")
                    continue
                else:
                    # update reco_record
                    self.reco_record.update_record(userid, sids=reco)

                w.write(
                    f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco)},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")
