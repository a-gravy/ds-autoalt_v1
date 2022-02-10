import os, logging
import yaml
import pickle
from autoalts.utils import efficient_reading
from autoalts.duplicates_prevention import RecoRecord

logging.basicConfig(level=logging.INFO)


class AutoAltMaker(object):
    def __init__(self, alt_info, create_date, blacklist_path=None, series_path=None, record_path=None, max_nb_reco=20, min_nb_reco=4):
        self.alt_info = alt_info
        self.create_date = create_date
        self.max_nb_reco = int(max_nb_reco)
        self.min_nb_reco = int(min_nb_reco)

        self.config = None
        with open("config.yaml") as f:
            self.config = yaml.load(f.read(), Loader=yaml.FullLoader)

        self.blacklist = set()
        if blacklist_path:
            with open(blacklist_path, "r") as r:
                while True:
                    line = r.readline()
                    if line:
                        self.blacklist.add(line.rstrip())
                    else:
                        break
            logging.info(f"{len(self.blacklist)} blacklist SIDs load")
        else:
            logging.info("no blacklist filtering")

        # series dict
        if series_path:
            self.series_dict = self.read_series_dict(series_path)
            logging.info("series_dict loaded -> will remove same series SID")
        else:
            self.series_dict = None
            logging.info("no series_dict -> won't remove series SIDs")

        self.reco_record = RecoRecord(record_range=5)
        if record_path:
            self.reco_record.read_record(record_path)

    def output_reco(self, userid, reco):
        return f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco[:self.max_nb_reco])},"f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1,"f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n"

    def remove_black_duplicates(self, userid, sid_list):
        # remove blacklist & sids got reco already with the same sid order
        rm_sids = self.blacklist | self.reco_record.get_record(userid)
        return [SID for SID in sid_list if SID not in rm_sids]

    def black_list_filtering(self, SIDs):
        """
        given a SID list, return a list which is filtered out blacklist sid
        :param SIDs:
        :return:
        """
        if not SIDs:
            logging.debug("SIDs is empty")
            return []
        filtered_SIDs = [SID for SID in SIDs if SID not in self.blacklist]
        if len(SIDs) - len(filtered_SIDs) != 0:
            logging.debug(f"[black_list_filtering]: from {len(SIDs)} to {len(filtered_SIDs)}")
        return filtered_SIDs

    def read_target_users(self, target_users_path):
        target_users = []
        if target_users_path:
            for line in efficient_reading(target_users_path):
                target_users.append(line.rstrip())
            logging.info(f"read {len(target_users)} target users. ({target_users[:3]})")
        return target_users

    def rm_duplicates(self, SIDs):
        unqiues = []
        for SID in SIDs:
            if SID not in unqiues:
                unqiues.append(SID)
        if len(SIDs) - len(unqiues) != 0:
            logging.info(f"[rm_duplicates]: from {len(SIDs)} to {len(unqiues)}")
        return unqiues

    def read_series_dict(self, input_path):
        series_dict = {}
        with open(input_path, "r") as r:  # sakuhin_public_code,series_id,series_in_order
            print(r.readline())
            while True:
                line = r.readline()
                if line:
                    arr = line.rstrip().replace('"','').split(",")
                    if arr[1]:
                        series_dict[arr[0]] = arr[1]
                else:
                    break
        logging.info(f"read {len(series_dict)} dict(sid:series_id) done ")
        return series_dict

    def rm_series(self, SIDs):
        if not self.series_dict:
            raise Exception("ERROR, should input series file path")
        series_pool = set()
        reco_item_list = []

        for sid in SIDs:
            series_id = self.series_dict.get(sid, None)
            if series_id:
                if series_id in series_pool:
                    continue
                else:
                    series_pool.add(series_id)
                    reco_item_list.append(sid)
            else:
                reco_item_list.append(sid)
        if len(SIDs) - len(reco_item_list) != 0:
            logging.debug(f"[rm_series] from {len(SIDs)} to {len(reco_item_list)}")

        return reco_item_list

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

    def make_alt(self, **kwargs):
        raise Exception("Unimplemented Error")
    '''
    def read_already_reco_sids(self, already_reco_path):
        self.already_reco_dict = {}  # format: userid, SID|SID|...
        for line in efficient_reading(already_reco_path, True, "userid,sid_list"):
            arr = line.rstrip().split(",")
            self.already_reco_dict[arr[0]] = set(arr[1].split('|'))  # arr[1]
        logging.info(f"[read_already_reco_sids] {len(self.already_reco_dict)} users read")

    def rm_already_reco_sids(self, userid, sid_list):
        rm_sids = self.already_reco_dict.get(userid, None)
        if rm_sids:
            return [sid for sid in sid_list if sid not in rm_sids]
        else:
            return sid_list
    '''


