import pandas as pd
from autoalt_maker import AutoAltMaker


class DailyTop(AutoAltMaker):
    def __init__(self, alt_info, create_date, blacklist_path, series_path=None, max_nb_reco=30, min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)

    def make_alt(self, input_path):
        if self.alt_info['domain'].values[0] == "video":
            self.video_domain_genre(input_path)
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception("unknown ALT_domain")

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

        SIDs = self.black_list_filtering(SIDs)
        SIDs = self.rm_duplicates(SIDs)

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

        reco_str = '|'.join(reco[:self.max_nb_reco])

        # just make one row for user "COMMON"
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['autoalt'])
            w.write(f"COMMON,{self.alt_info['feature_public_code'].values[0]},{self.create_date},{reco_str},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1\n")

    def video_domain(self, input_path="data/daily_top_genre.csv"):
        """
        logic:
        make ALT_daily_top by order of daily_top.csv

        """
        pass
        """
        SIDs = []  # keep SID unique
        with open(input_path, 'r') as r:
            r.readline()  # skip the first line
            while True:
                line = r.readline()
                if line:  # ep,SID,nb_watch
                    SID = line.rstrip().split(",")[1].replace('"', '')
                    if SID not in SIDs:
                        SIDs.append(SID)

                        if len(SIDs) >= top_N:
                            break
                else:
                    break

        SIDs = list(SIDs)
        SIDs_str = '|'.join(SIDs[:int(top_N)])

        # just make one row for user "COMMON"
        with open(f"{alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(config['header']['autoalt'])
            w.write(f"COMMON,{alt_info['feature_public_code'].values[0]},{create_date},{SIDs_str},"
                    f"{alt_info['feature_title'].values[0]},{alt_info['domain'].values[0]},1\n")
        """
        """ 
        # this version make one row for each user
        user_count = 0
        with open(f"{feature_public_code}-{ALT_domain}.csv", "w") as w:
            with open("data/target_users.csv", "r") as r:
                r.readline()
                while True:
                    line = r.readline()
                    if line:
                        user_count += 1
                        arr = line.rstrip().split(",")
                        w.write("{},{},{:.4f}\n".format(arr[1], SIDs_str, 1.0))
                    else:
                        logging.info(f"{user_count} users' daily_top are updated")
                        break
        """
