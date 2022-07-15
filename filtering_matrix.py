import csv
import logging
import pickle
from tqdm import tqdm
from scipy.sparse import coo_matrix
from dscollaborative.recommender import ImplicitModel

logging.basicConfig(level=logging.INFO)


def merge_with_filtering_matrix(model, interaction_history_file):
    """
    1. update model.user_item_matrix to filtering matrix
    2. output to output_model_path
    """
    logging.info("csr -> lil converting")
    model_lil = model.user_item_matrix.matrix.tolil()
    logging.info(f"before merging, user-item interaction nb = {model_lil.count_nonzero()}")

    with open(interaction_history_file, "r") as csv_path:
        reader = csv.DictReader(csv_path)
        for line in tqdm(reader):
            users, sakuhins = (
                line["user_multi_account_id"],
                line["sakuhin_codes"].split("|"),
            )
            userid = model.user_item_matrix.user2id.get(users, None)
            if userid:
                for SID in sakuhins:
                    sid_idx = model.user_item_matrix.item2id.get(SID, None)
                    if sid_idx:
                        model_lil[userid, sid_idx] = 1
    logging.info(f"after merging, user-item interaction nb = {model_lil.count_nonzero()}")
    model.user_item_matrix.matrix = coo_matrix(model_lil).tocsr()


def make_filtering_matrix(model_path, interaction_history_file, output_model_path):
    model = pickle.load(open(model_path, "rb"))
    merge_with_filtering_matrix(model, interaction_history_file)
    pickle.dump(model, open(output_model_path, "wb"))


def make_filtering_matrix_v5(model_path, interaction_history_file, output_model_path):
    model = ImplicitModel()
    model.load_model(model_path)
    model.merge_with_filtering_matrix(interaction_history_file)
    model.dump_model(output_model_path)

def verification(model_path):
    model = pickle.load(open(model_path, "rb"))
    logging.info(f"user-item interaction nb = {len(model.user_item_matrix.matrix.nonzero()[0])}")


if __name__ == "__main__":
    make_filtering_matrix("data/als_model.latest", "data/interaction_history.csv", "data/als_model.filtering")
    logging.info("="*10)
    verification("data/als_model.filtering")

