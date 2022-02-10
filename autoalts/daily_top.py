import logging
import operator
from autoalts.autoalt_maker import AutoAltMaker
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


class DailyTop(AutoAltMaker):
    def __init__(self, **kwargs):
        super().__init__(alt_info=kwargs["alt_info"], create_date=kwargs["create_date"], blacklist_path=kwargs["blacklist_path"],
                         series_path=kwargs["series_path"], max_nb_reco=kwargs["max_nb_reco"], min_nb_reco=kwargs["min_nb_reco"])

    def make_alt(self, **kwarg):
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            if "SFET" in self.alt_info['feature_public_code'].values[0]:  # TODO: tmp place for coldstart
                self.video_domain(kwarg["input_path"])
            else:
                # self.video_domain_genre(input_path)
                self.video_domain(kwarg["input_path"])
        elif self.alt_info['domain'].values[0] == "semiadult":
            self.semi_adult(kwarg["input_path"])
        elif self.alt_info['domain'].values[0] == "adult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def semi_adult(self, input_path):
        SIDs = []  # keep SID unique

        with open(input_path, 'r') as r:
            r.readline()  # skip the first line
            while True:
                line = r.readline()  # "episode_code","SID","display_name","main_genre_code","nb_watch"
                if line:  # ep,SID,nb_watch
                    arr = line.rstrip().replace('"', '').split(",")
                    if arr[1] not in SIDs:
                        SIDs.append(arr[1])
                else:
                    break

        if not SIDs:
            raise Exception(f"ERROR:{input_path} has no data")
        else:
            logging.info(f"read {len(SIDs)} lines, going to make FET daily_top ")

        reco_str = '|'.join(SIDs[:self.max_nb_reco])
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            w.write(f"COMMON,{self.alt_info['feature_public_code'].values[0]},{self.create_date},{reco_str},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

    def video_domain_genre(self, input_path):
        """
        logic:
        make ALT_daily_top by order and genre
        """
        SIDs = []  # keep SID unique
        genres = []

        with open(input_path, 'r') as r:
            r.readline()  # skip the first line
            while True:
                line = r.readline()  # "episode_code","SID","display_name","main_genre_code","nb_watch"
                if line:  # ep,SID,nb_watch
                    arr = line.rstrip().replace('"', '').split(",")
                    SIDs.append(arr[1])
                    genres.append(arr[3])
                else:
                    break

        if not SIDs:
            raise Exception(f"ERROR:{input_path} has no data")
        else:
            logging.info(f"read {len(SIDs)} lines, going to make FET daily_top ")

        genre_dict = {}  # genre: SID list
        for SID, genre in zip(SIDs, genres):
            SID_list = genre_dict.get(genre, None)
            if not SID_list:
                genre_dict.setdefault(genre, [SID])
            elif SID not in SID_list:
                genre_dict[genre] = SID_list + [SID]

        max_len = 0  # longest length of
        for v in genre_dict.values():
            max_len = max(max_len, len(v))

        # make recommendation by order and genre
        for k in genre_dict.keys():
            genre_dict[k] = genre_dict[k] + [None] * (max_len - len(genre_dict[k]))

        reco = []
        for i in range(max_len):
            for k in genre_dict.keys():
                if genre_dict[k][i]:
                    reco.append(genre_dict[k][i])

        reco = self.black_list_filtering(reco)
        reco = self.rm_duplicates(reco)

        reco_str = '|'.join(reco[:self.max_nb_reco])

        # just make one row for user "COMMON"
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            w.write(f"COMMON,{self.alt_info['feature_public_code'].values[0]},{self.create_date},{reco_str},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

    def video_domain(self, input_path):
        """
        logic:
        make ALT_daily_top by order of nb_watch sum using GP datamart

        """
        sid_list = []

        for line in efficient_reading(input_path, True, "sakuhin_public_code,sakuhin_name,uu,today_top_rank"):
            sid_list.append(line.split(",")[0])

        reco_str = '|'.join(sid_list[:self.max_nb_reco])
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            w.write(f"COMMON,{self.alt_info['feature_public_code'].values[0]},{self.create_date},{reco_str},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

    def video_domain_td(self, input_path):
        """
        logic:
        make ALT_daily_top by order of nb_watch sum  using td data

        """
        sid_nb = {}  # SID: nb of watch

        # accumulate nb_of_watch of each EP
        with open(input_path, "r") as r:
            r.readline()
            while True:
                line = r.readline()
                if line:
                    arr = line.rstrip().split(",")
                    sid_nb[arr[1]] = sid_nb.setdefault(arr[1], 0) + int(arr[-1])
                else:
                    break

        reco = [k for k, v in sorted(sid_nb.items(), key=operator.itemgetter(1), reverse=True)]
        reco = self.black_list_filtering(reco)
        reco = self.rm_duplicates(reco)

        reco_str = '|'.join(reco[:self.max_nb_reco])
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            # TODO: user_multi_account_id
            w.write(f"COMMON,{self.alt_info['feature_public_code'].values[0]},{self.create_date},{reco_str},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"
                    f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")
