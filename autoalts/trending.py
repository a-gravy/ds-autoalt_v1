import logging
from autoalts.basic_alt import BasicALT
from autoalts.utils import efficient_reading
logging.basicConfig(level=logging.INFO)


class Trending(BasicALT):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def read_pool(self, pool_path):
        pool_SIDs = set()
        # "episode_code,SID,display_name,main_genre_code,nb_watch"
        for line in efficient_reading(pool_path, True, "today_top_rank,sakuhin_public_code,sakuhin_name,uu,zenkai_uu,trend_perc"):
            arr = line.rstrip().split(",")
            if float(arr[-1]) < 1.0:
                break
            pool_SIDs.add(line.split(",")[1])
        return pool_SIDs
