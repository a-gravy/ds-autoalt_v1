import logging
from autoalts.basic_alt import BasicALT
from autoalts.utils import efficient_reading
logging.basicConfig(level=logging.INFO)


class Popular(BasicALT):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def read_pool(self, pool_path):
        pool_SIDs = set()
        for line in efficient_reading(pool_path, True, "main_genre_code,sakuhin_public_code,sakuhin_name,row_th"):
            pool_SIDs.add(line.split(",")[1])
        return pool_SIDs
