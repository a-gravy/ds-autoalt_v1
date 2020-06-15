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

        self.filter_items = file_to_list(self.filter_items_path) if self.filter_items_path is not None else {}

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

    def new_arrival(self, ALT_code="ALT000001", alt_genre="SOUGOU",
                    input_path="data/new_arrival.csv", output_path="new_arrival_reranked.csv",
                    ):
        """
        input file name: new_arrival.csv
        input format: sakuhin_public_code,production_year,main_genre_code,sale_start_datetime,ftu,uu

        """
        # logic 1. just rerank using user's score
        target_items = []
        with open(input_path, 'r') as r:
            r.readline()  # skip the first line
            while True:
                line = r.readline()
                if line:
                    target_items.append(line.rstrip().split(",")[0])
                else:
                    break

        logging.info("{} of new-arrival items as target items loaded".format(len(target_items)))

        with open(output_path, "w") as w:
            w.write("user_multi_account_id,ALT_code,alt_genre,sakuhin_codes,alt_score\n")
            for i, (uid, reranked_list, score_list) in enumerate(
                    rerank(model=self.model, target_users=self.target_users,
                           target_items=target_items, filter_already_liked_items=False)):

                if i % 1000 == 0:
                    logging.info("{}/{} = {:.1f}% done".format(i, len(self.model.user_item_matrix.user2id),
                                                               100 * float(i) / len(
                                                                   self.model.user_item_matrix.user2id)))

                reranked_tuple = [(sid, score) for sid, score in zip(reranked_list, score_list)
                                  if sid not in set(self.filter_items) and
                                  sid not in set(self.dict_watched_sakuhin.get(uid, []))]
                reranked_list = [sid for (sid, score) in reranked_tuple]
                score_list = [score for (sid, score) in reranked_tuple]

                if len(reranked_list) < self.nb_reco:
                    logging.info("[{}] WARNING {} is too few {}".format(alt_genre, uid, len(reranked_list)))
                    continue

                reranked_list = reranked_list[:self.nb_reco]
                score_list = score_list[:self.nb_reco]

                w.write("{},{},{},{},{:.4f}\n".format(uid, ALT_code, alt_genre, "|".join(reranked_list),
                                                      sum(score_list) / float(len(score_list))))

    def new_arrival_by_genre(self):
        """
        input file: new_arrival_genre.csv
        input format: sakuhin_public_code,production_year,main_genre_code,sale_start_datetime,recency,ftu,uu
        :return:
        """
        input_path = "../ds-auto_altmakers/new_arrival_genre.csv"
        output_path = "alts/new_arrival_reranked.csv"

        arrival_alt_lookup = {
            'ALT000002': 'YOUGA',
            'ALT000003': 'HOUGA',
            'ALT000004': 'ANIME',
            'ALT000005': 'FDRAMA',
            'ALT000006': 'VARIETY',
            'ALT000007': 'MUSIC_IDOL',
            'ALT000008': 'SPORT',
            'ALT000009': 'DRAMA',
            'ALT000010': 'ADRAMA',
            'ALT000011': 'NEWS',
            'ALT000012': 'DOCUMENT',
            'ALT000013': 'KIDS',
        }
        alt_code_lookup = {v: k for k, v in arrival_alt_lookup.items()}  # inverse for lookup

        # read input
        genre_dict = {}  # genre_name:[sid list]
        with open(input_path, "r") as r:
            r.readline()  # skip first line
            while True:
                line = r.readline()
                if line:
                    arrs = line.rstrip().split(",")
                    genre_dict[arrs[2]] = genre_dict.setdefault(arrs[2], []) + [arrs[0]]
                else:
                    break

        self.output_for_cassandra(genre_dict, alt_code_lookup, output_path)

    def weekly_top(self):
        """
        input:
        sakuhin_public_code,sakuhin_name,main_genre_code,ftu,uu
        SID0044256,pet2,YOUGA,12,474
        -----
        issue: wrong genre -> workaround: filtered by genre_list
        2020-02-21 12:07:15,858 - root - INFO - wrong genre  サイモン 17歳の告白"
2020-02-21 12:07:15,864 - root - INFO - wrong genre ロングバケーション"
2020-02-21 12:07:15,875 - root - INFO - wrong genre トーニャ 史上最大のスキャンダル"
2020-02-21 12:07:15,881 - root - INFO - wrong genre  My Love"
2020-02-21 12:07:15,889 - root - INFO - wrong genre 000デイズ・オン・アース"
2020-02-21 12:07:15,896 - root - INFO - wrong genre BLOOD」"
2020-02-21 12:07:15,897 - root - INFO - wrong genre  with you』"
2020-02-21 12:07:15,900 - root - INFO - wrong genre 若者さまCH あゝバズりたい"
2020-02-21 12:07:15,902 - root - INFO - wrong genre Tight』"
2020-02-21 12:07:15,907 - root - INFO - wrong genre oh my』"
2020-02-21 12:07:15,908 - root - INFO - wrong genre  PUNK'N ROLL NIGHT"
2020-02-21 12:07:15,909 - root - INFO - wrong genre  I Love Ya！"
2020-02-21 12:07:15,932 - root - INFO - wrong genre  No Gain」"
2020-02-21 12:07:15,933 - root - INFO - wrong genre BLOOD"
2020-02-21 12:07:15,933 - root - INFO - wrong genre  Girls！ 新章"
2020-02-21 12:07:15,937 - root - INFO - wrong genre  Girls！"
2020-02-21 12:07:15,939 - root - INFO - wrong genre  Girls！ 続・劇場版 前篇 青春の影"
2020-02-21 12:07:15,939 - root - INFO - wrong genre  Girls！ 七人のアイドル"
2020-02-21 12:07:15,946 - root - INFO - wrong genre  Girls！ 続・劇場版 後篇 Beyond the Bottom"
2020-02-21 12:07:15,947 - root - INFO - wrong genre  Jekyll
2020-02-21 12:07:15,948 - root - INFO - wrong genre  but how you say it"
2020-02-21 12:07:15,949 - root - INFO - wrong genre  Like Clouds
        """
        input_path = "../ds-auto_altmakers/weekly_top.csv"
        output_path = "alts/weekly_top_reranked.csv"

        # read input
        genre_dict = {}  # genre_name:[sid list]
        sougou_list = []  # for SOUGOU
        alt_code_lookup = {
            'YOUGA': 'ALT000014',
            'HOUGA': 'ALT000015',
            'ANIME': 'ALT000016',
            'FDRAMA': 'ALT000017',
            'VARIETY': 'ALT000018',
            'MUSIC_IDOL': 'ALT000019',
            'SPORT': 'ALT000020',
            'DRAMA': 'ALT000021',
            'ADRAMA': 'ALT000022',
            'NEWS': 'ALT000023',
            'DOCUMENT': 'ALT000024',
            'KIDS': 'ALT000025',
            'SOUGOU': 'ALT000026'
        }

        # TODO: ' Jekyll': ['SID0020693'], ' but how you say it"': ['SID0036567'], ' Like Clouds': ['SID0036566']}
        with open(input_path, "r") as r:
            r.readline()  # skip first line
            while True:
                line = r.readline()
                if line:
                    arrs = line.rstrip().split(",")
                    if arrs[2] in genre_list:
                        genre_dict[arrs[2]] = genre_dict.setdefault(arrs[2], []) + [arrs[0]]
                        sougou_list.append(arrs[0])
                    else:
                        logging.info("wrong genre {}".format(arrs[2]))
                else:
                    break
        # SOUGOU for main page
        genre_dict["SOUGOU"] = sougou_list

        self.output_for_cassandra(genre_dict, alt_code_lookup, output_path)

    def trending(self):
        """
        input:
        uu_improve,uu,sakuhin_public_code,main_genre_code,display_name
        183.07,357,SID0034268,ADRAMA,Bravo! Your life~
        -----
        logic 1. SOUGOU is decided by bpr score (now)
        logic 2. SOUGOU is decided by top N of uu_improve or uu
        """
        input_path = "trending.csv"
        output_path = "alts/trending_reranked.csv"
        alt_code_lookup = {
            'SOUGOU': 'ALT000028',
            'YOUGA': 'ALT000029',
            'HOUGA': 'ALT000030',
            'ANIME': 'ALT000031',
            'FDRAMA': 'ALT000032',
            'VARIETY': 'ALT000033',
            'MUSIC_IDOL': 'ALT000034',
            'SPORT': 'ALT000035',
            'DRAMA': 'ALT000036',
            'ADRAMA': 'ALT000037',
            'NEWS': 'ALT000038',
            'DOCUMENT': 'ALT000039',
            'KIDS': 'ALT000040',
        }

        # read input
        genre_dict = {}  # genre_name:[sid list]
        sougou_list = []
        with open(input_path, "r") as r:
            r.readline()  # skip first line
            while True:
                line = r.readline()
                if line:
                    arrs = line.rstrip().split(",")
                    if arrs[3] in genre_list:
                        genre_dict[arrs[3]] = genre_dict.setdefault(arrs[3], []) + [arrs[2]]
                        sougou_list.append(arrs[2])
                    else:
                        logging.info("wrong genre {}".format(arrs[3]))
                else:
                    break
        # SOUGOU for main page
        genre_dict["SOUGOU"] = sougou_list

        self.output_for_cassandra(genre_dict, alt_code_lookup, output_path)

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
                            reranked_list = [sid for sid in reranked_list
                                             if sid not in set(self.filter_items) and
                                             sid not in set(self.dict_watched_sakuhin.get(uid, []))]

                            if len(reranked_list) < self.min_nb_reco:
                                continue

                            w.write("{},{},{},{:.4f}\n".format(uid, arrs[0],
                                                               "|".join(reranked_list[:self.max_nb_reco]), alt_score_list[i]))
                            line_written+=1
                        nb_genrealt_done += 1
                    else:
                        break

        # table for dim_alt_genre_row  TODO: do we need this?
        """
        with open("dim_alt_genre_rows.csv", 'w') as w:
            today = pd.Timestamp.today().strftime("%Y-%m-%d")
            w.write("ALT_code,alt_type,alt_title,alt_description,main_genre_code,update_time\n")
            for i, (alt, ALT_code) in enumerate(alt_dict.items()):
                w.write(
                    "{},genre_row,{},あなたの視聴履歴から,SOUGOU,{}\n".format(
                        ALT_code, alt, today))
        """

    def output_for_cassandra(self, genre_dict, alt_code_lookup, output_path):
        with open(output_path, "a") as w:
            w.write("user_multi_account_id,ALT_code,alt_genre,sakuhin_codes,alt_score\n")
            for alt_genre, target_items in genre_dict.items():
                ALT_code = alt_code_lookup[alt_genre]
                for i, (uid, reranked_list, score_list) in enumerate(
                        rerank(model=self.model, target_users=self.target_users,
                               target_items=target_items,
                               filter_already_liked_items=False,
                               batch_size=5000)):

                    if i % 1000 == 0:
                        logging.info("{}/{} = {:.1f}% done".format(i, len(self.target_users),
                                                                   100 * float(i) / len(self.target_users)))

                    # item filtering
                    reranked_tuple = [(sid, score) for sid, score in zip(reranked_list, score_list)
                                      if sid not in set(self.filter_items) and
                                      sid not in set(self.dict_watched_sakuhin.get(uid, []))]
                    reranked_list = [sid for (sid, score) in reranked_tuple]
                    score_list = [score for (sid, score) in reranked_tuple]

                    if len(reranked_list) < self.min_nb_reco:
                        logging.info("[{}] WARNING {} is too few {}".format(alt_genre, uid, len(reranked_list)))
                        continue

                    reranked_list = reranked_list[:self.max_nb_reco]
                    score_list = score_list[:self.max_nb_reco]

                    w.write(
                        "{},{},{},{},{:.4f}\n".format(uid, ALT_code, alt_genre, "|".join(reranked_list),
                                                      sum(score_list) / float(len(score_list))))


def demo_last_similar_sakuhin():
    """
    user_multi_account_id,sakuhin_public_code,display_name

    output =
    * [for starship] UID,ALTUID,SOUGOU,sakuhin_code,alt_score
    * [.sql to insert row to dim_alt]
    ALTUID,'dragon ballを見たあなたへ','ぜひご覧ください',CONTENT_BASED','USER_WATCHED','SOUGOU','PERSONALIZED' for alt.dim_alt
    """
    cbf_dict = {}
    with open("../ippan_verification/cbf_rs_list.csv", "r") as r:
        for line in r.readlines():
            arrs = line.rstrip().split(",")
            cbf_dict.setdefault(arrs[0], arrs[1])

    with open("../ds-auto_altmakers/last_watched_meta.csv", "r") as r:
        with open("alts/last_watched_alts.csv", "w") as w_cass:
            with open("alts/last_watched.sql", "w") as w_sql:
                w_sql.write("insert into alt.dim_alt_r values\n")
                r.readline()
                for line in r.readlines():
                    arrs = line.rstrip().split(",")
                    uid = arrs[0]
                    ALT_code = "ALT{}".format(uid)
                    sakuhin_codes = cbf_dict.get(arrs[1], None)
                    if sakuhin_codes:
                        w_cass.write("{},{},SOUGOU,{},{:.1f}\n".format(uid, ALT_code, cbf_dict[arrs[1]], 10))
                    else:
                        print("{} not found".format(arrs[1]))

                    w_sql.write(
                        "'{}','[{}]を見たあなたへ','ぜひご覧ください','CONTENT_BASED','USER_WATCHED','SOUGOU','PERSONALIZED',now()),\n".format(
                            ALT_code, arrs[2].replace("'", "")))


config = {
    "ALT000001": {},
}

genre_list = {'SOUGOU', 'YOUGA', 'HOUGA', 'ANIME', 'FDRAMA', 'VARIETY', 'MUSIC_IDOL', 'SPORT', 'DRAMA', 'ADRAMA',
              'NEWS', 'DOCUMENT', 'KIDS'}


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
