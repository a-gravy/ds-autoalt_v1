import os, logging
import yaml
import pickle

logging.basicConfig(level=logging.INFO)


class AutoAltMaker(object):
    def __init__(self, alt_info, create_date, blacklist_path, max_nb_reco=30, min_nb_reco=3):
        self.alt_info = alt_info
        self.create_date = create_date
        self.max_nb_reco = int(max_nb_reco)
        self.min_nb_reco = int(min_nb_reco)

        self.config = None
        with open("config.yaml") as f:
            self.config = yaml.load(f.read(), Loader=yaml.FullLoader)

        self.blacklist = set()
        with open(blacklist_path, "r") as r:
            while True:
                line = r.readline()
                if line:
                    self.blacklist.add(line.rstrip())
                else:
                    break
        logging.info(f"{len(self.blacklist)} blacklist SIDs load")

        """
        # series dict
        if series_path:
            series_dict = self.read_series_dict(series_path)
            logging.info("series_dict loaded")
        else:
            series_dict = None
            logging.info("no series_dict -> won't remove series SIDs")
        """

    def black_list_filtering(self, SIDs):
        """
        given a SID list, return a list which is filtered out blacklist sid
        :param SIDs:
        :return:
        """
        filtered_SIDs = [SID for SID in SIDs if SID not in self.blacklist]
        logging.info(f"black_list_filtering SIDs: from {len(SIDs)} to {len(filtered_SIDs)}")
        return filtered_SIDs

    def rm_duplicates(self, SIDs):
        unqiues = []
        for SID in SIDs:
            if SID not in unqiues:
                unqiues.append(SID)
        if len(SIDs) - len(unqiues) == 0:
            pass
        else:
            logging.info(f"rm_duplicates: from {len(SIDs)} to {len(unqiues)}")
        return unqiues

    def check_reco(self, reco_path):
        unique_sid_pool = set()
        # user_multi_account_id,feature_public_code,create_date,sakuhin_codes,feature_title,domain,autoalt
        with open(reco_path, "r") as r:
            r.readline()
            while True:
                line = r.readline()
                if line:
                    SIDs = line.split(",")[3].split("|")
                    for sid in SIDs:
                        if sid in self.blacklist:
                            raise Exception(f"[black_list] {sid} in {line}")
                        if sid not in unique_sid_pool:
                            unique_sid_pool.add(sid)
                        else:
                            raise Exception(f"[duplicates] duplicated {sid}")
                else:
                    break
        logging.info(f"{reco_path} passes blacklist and duplicates check, good to go")

    def load_model(self, path):
        return pickle.load(open(path, "rb"))

    def batch(self, iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    def make_alt(self):
        raise Exception("Unimplemented Error")


