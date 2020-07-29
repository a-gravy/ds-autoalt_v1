"""alt_reranker

Usage:
    alt_reranker.py (new_arrival) --model=PATH <ALT_code> <alt_genre> [--top_n=<tn> | --target_users=PATH]
    alt_reranker.py top <ALT_code> <alt_genre> --target_users=PATH

Options:
    -h --help Show this screen
    --version
    --model PATH         File path location of the trained model
    --top_n=<tn>         Number of recommended items. [default: 10]
    ALT_code      detail@dim_alt
    alt_genre            SOUGOU for main page, genre name for each genre


"""
import os, logging
import pandas as pd
from dstools.logging import setup_logging
from dstools.utils import normalize_path, save_list_to_file, file_to_list
from bpr.implicit_recommender import rerank, load_model
from docopt import docopt
from dstools.cli.parser import parse

logger = logging.getLogger(__name__)


# TODO: it is for all users before A/B testing, need to change format if we need to do A/B testing
def read_target_user(input_path):
    # format = pfid,uid,super_user
    pfid_list, uid_list, is_super_user_list = [], [], []
    with open(input_path, 'r') as r:
        r.readline()  # skip header
        while True:
            line = r.readline()
            if line:
                arrs = line.rstrip().split(",")
                pfid_list.append(arrs[0])
                uid_list.append(arrs[1])
                is_super_user_list.append(arrs[2])
            else:
                break
    return pfid_list, uid_list, is_super_user_list


class alt_reranker:
    """
    kwargs = {
            "model_path": arguments["--model"],
            "target_users_path": arguments.get("--target_users", None),
            "target_items_path": arguments.get("--target_items", None),
            "filter_items": arguments.get("--filter_items", None),
            "watched_list_rerank": arguments.get("--watched_list", None),
            "max_nb_reco": int(arguments["--max_nb_reco"]),
            "min_nb_reco": int(arguments["--min_nb_reco"]),
        }
    """

    def __init__(self, model_path, target_users_path, target_items_path, filter_items, watched_list_rerank,
                 max_nb_reco, min_nb_reco):
        os.makedirs("./data", exist_ok=True)
        self.model_path = normalize_path(model_path, error_if_not_exist=True)
        self.model = load_model(self.model_path)
        # self.out_path = normalize_path(opts.get("out"))
        # a list of target userid(ex: PM015212809); None = all users
        self.target_users_path = normalize_path(target_users_path)
        # a list of target itemid(ex: SID0041816); None = all items
        self.target_items_path = normalize_path(target_items_path)
        self.filter_items_path = normalize_path(filter_items)
        self.watched_list_rerank_path = normalize_path(watched_list_rerank)
        # self.series_rel_path = normalize_path(opts.get("series_rel", None))
        self.max_nb_reco = max_nb_reco
        self.min_nb_reco = min_nb_reco

        # TODO consider make self.target_users as set
        # _, self.target_users, _ = read_target_user(self.target_users_path)
        self.target_users = file_to_list(self.target_users_path) if self.target_users_path is not None else self.model.user_item_matrix.user2id
        self.target_items = file_to_list(self.target_items_path) if self.target_items_path is not None else None

        logging.info("using model for {} users and {} items".format(
            len(self.target_users) if self.target_users else len(self.model.user_item_matrix.user2id),
            len(self.target_items) if self.target_items else len(self.model.user_item_matrix.item2id)
        ))

        self.filter_items = set(file_to_list(self.filter_items_path)) if self.filter_items_path is not None else set()

        # for recording user seen SIDs, {user_id: [SIDs]}
        self.dict_watched_sakuhin = {}
        if self.watched_list_rerank_path is not None:
            list_watched = file_to_list(self.watched_list_rerank_path)
            for i in list_watched:
                userid, sids = i.split(',')
                self.dict_watched_sakuhin[userid] = sids.split("|")
            del list_watched

        """
        self.series_dict = {}
        if self.series_rel_path:
            series_df = pd.read_csv(self.series_rel_path)
            for row in series_df.iterrows():
                self.series_dict[row[1]["sakuhin_public_code"]] = row[1]["series_id"]
            del series_df
        """

    def genre_rows(self, input_path="data/genre_rows.csv", output_path="genre_rows_reranked.csv"):
        """
        run reranking on genre_alts for personalization

        input:
        genre_features, sid list, uid list, score list
        type-DRAMA-genre-romance, SID|SID|..., user_nulti_account_id|..., score|score|...


        [for starship]
        user_multi_account_id,genre_features,SID|SID|...  ,score|score|...

        """
        with open(output_path, "w") as w:
            with open(input_path, "r") as r:
                nb_genrealt_done=0
                line_written = 0
                while True:
                    line = r.readline()

                    if line:
                        arrs = line.rstrip().split(",")
                        if 1:  # counter%100==0:
                            logging.info(f"{arrs[0]} ing, {nb_genrealt_done} alt done with {line_written} lines written")

                        # this score is feature score from user history statistics, use it instead of bpr score
                        # TODO: score = statistics + bpr_score
                        alt_score_list = [float(s) for s in arrs[3].split("|")]

                        for i, (uid, reranked_list, _) in enumerate(
                                rerank(model=self.model, target_users=arrs[2].split("|"),
                                       target_items=arrs[1].split("|"), filter_already_liked_items=False,
                                       batch_size=1000)):

                            # item filtering
                            block_set = self.filter_items | set(self.dict_watched_sakuhin.get(uid, []))
                            reranked_list = [sid for sid in reranked_list if sid not in block_set]
                            #reranked_list = [sid for sid in reranked_list
                            #                 if sid not in set(self.filter_items) and
                            #                 sid not in set(self.dict_watched_sakuhin.get(uid, []))]

                            # avoid duplicated SIDs in first 5 sakuhins -> add first 5 sakuhins to dict_watched_sakuhin[uid]
                            self.dict_watched_sakuhin[uid] = self.dict_watched_sakuhin.setdefault(uid, []) + reranked_list[:5]

                            if len(reranked_list) < self.min_nb_reco:
                                continue

                            w.write("{},{},{},{:.4f}\n".format(uid, arrs[0],
                                                               "|".join(reranked_list[:self.max_nb_reco]), alt_score_list[i]))
                            line_written+=1
                        nb_genrealt_done += 1
                    else:
                        break


def main():
    arguments = docopt(__doc__, version='0.9.0')
    cmd, opts = parse(arguments)
    logger.info(f"Executing '{cmd}' with arguments {opts}")
    # alt_reranker(opts).trending()
    # alt_reranker(opts).weekly_top()
    # alt_reranker(opts).history_based_alts()
    # demo_last_similar_sakuhin()


if __name__ == '__main__':
    main()
