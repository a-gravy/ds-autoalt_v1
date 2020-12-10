import logging
from autoalts.autoalt_maker import AutoAltMaker

logging.basicConfig(level=logging.INFO)



class ColdStartExclusive(AutoAltMaker):
    def __init__(self, alt_info, create_date, blacklist_path=None, series_path=None, max_nb_reco=30, min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)

    def make_alt(self, input=None):
        # 1. read csv: "sakuhin_public_code","display_name","MAIN_GENRE_CODE","DISPLAY_PRODUCTION_COUNTRY","POPULARITY_POINT","exclusive_start_datetime","exclusive_end_datetime","exclusive_type_code","exclusive_type_name"
        SIDs = []
        with open(input, 'r') as r:
            r.readline()
            for line in r.readlines():
                SIDs.append(line.split(",")[0].replace('"', ''))

        # 2. output by standard format
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['autoalt'])
            w.write(f"exclusive,{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(SIDs)},"
                f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1\n")









