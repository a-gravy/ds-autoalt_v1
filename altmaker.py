"""altmaker

Usage:
    altmaker.py new_arrival <ALT_code> <ALT_domain> --model=PATH [--top_n=<tn>]
    altmaker.py top <ALT_code> <ALT_domain> [--top_n=<tn>]
    altmaker.py byw <ALT_code> <ALT_domain> [--min_nb_reco=<tn> --filter_items=PATH  --watched_list=PATH]
    altmaker.py genre_row <ALT_code> <ALT_domain> --model=PATH [--min_nb_reco=<tn> --nb_alt=<nbgr>  --max_nb_reco=<nbr> --target_users=PATH --target_items=PATH --filter_items=PATH  --watched_list=PATH]

Options:
    -h --help Show this screen
    --version
    --model PATH          File path location of the trained model
    --top_n=<tn>          Number of recommended items. [default: 10]
    --max_nb_reco=<nbr>   Maximal number of items in one ALT [default: 30]
    --min_nb_reco=<tn>    Minimal number of items in one ALT [default: 3]
    --nb_alt=<nbgr>       how many alts made for each user  [default: 3]
    ALT_code              detail@dim_alt
    ALT_domain            SOUGOU, movie, book, manga, music … etc. SOUGOU means mixing all ALT_domain types together
    --filter_items PATH   filter_out_sakuhin_implicit.csv
    --watched_list PATH   watched_list_rerank.csv
    --target_users PATH   active users
    --target_items PATH   target items to recommend


"""
"""
output of each ALT
file_name:<ALT_code>-<ALT_domain>.csv 

format:  
* no head
* user_multi_account_id,SID|SID|...  , alt_score
* byw: user_multi_account_id,byw_sid,SID|SID|...  ,alt_score
* genre: user_multi_account_id,type-VARIETY-nations-日本,SID|SID|...,alt_score(from genre, not bpr)

"""
import os, logging, time
from docopt import docopt
from dstools.logging import setup_logging
import alt_reranker
from new_arrival import make_alt as new_arrival_maker
from because_you_watched import make_alt as byw_maker
from genre_row_maker import make_alt as genre_row_maker


setup_logging()
logger = logging.getLogger(__name__)


def daily_top(top_N = 10, ALT_code="ATL_daily_top", ALT_domain="video_all",
              input_path="data/daily_top.csv"):
    if ALT_domain == "video_all":
        SIDs = []
        with open(input_path, 'r') as r:
            r.readline()  # skip the first line
            while True:
                line = r.readline()
                if line:
                    SIDs.append(line.rstrip().split(",")[0])
                else:
                    break

        with open(f"{ALT_code}-{ALT_domain}.csv", "w") as w:
            w.write("{},{},{:.4f}\n".format("all", '|'.join(SIDs[:int(top_N)]), 1.0))
    elif ALT_domain == "book":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    start_time = time.time()

    if arguments['new_arrival']:
        new_arrival_maker(arguments['--model'], ALT_code=arguments["<ALT_code>"], ALT_domain=arguments["<ALT_domain>"])
        # altmaker.py (new_arrival) --model=PATH <ALT_code> <alt_genre> [--top_n=<tn> | --target_users=PATH]
        # alt_reranker.alt_reranker(opts).new_arrival(input_path="data/new_arrival.csv", ALT_code=arguments["<ALT_code>"], alt_genre=arguments["<alt_genre>"])
    elif arguments['top']:
        daily_top(top_N=int(arguments['--top_n']), input_path="data/daily_top.csv",
                  ALT_code=arguments["<ALT_code>"], ALT_domain=arguments["<ALT_domain>"])
    elif arguments['byw']:
        # python altmaker.py byw ALT_byw video_all --filter_items data/filter_out_sakuhin_implicit.csv --watched_list data/watched_list_rerank.csv --min_nb_reco 4
        kwargs = {
            "ALT_code": arguments["<ALT_code>"],
            "ALT_domain": arguments["<ALT_domain>"],
            "min_nb_reco": int(arguments["--min_nb_reco"]),
            "filter_items": arguments.get("--filter_items", None),
            "watched_list_rerank": arguments.get("--watched_list", None),
            "user_sessions_path": 'data/new_user_sessions.csv',
            "cbf_table_path": "data/cbf_integration.csv"
        }
        byw_maker(**kwargs)
    elif arguments['genre_row']:
        # python altmaker.py genre_row ALT_genrerow video_all --model ../data/bpr/implicit_bpr.model.2020-06-06 --target_users data/target_users.csv
        kwargs = {
            "ALT_code": arguments["<ALT_code>"],
            "ALT_domain": arguments["<ALT_domain>"],
            "nb_alt": int(arguments["--nb_alt"]),
            "max_nb_reco": int(arguments["--max_nb_reco"]),
            "min_nb_reco": int(arguments["--min_nb_reco"]),
            "unext_sakuhin_meta_path": "data/unext_sakuhin_meta.csv",
            "meta_lookup_path": "data/unext_sakuhin_meta_lookup.csv",
            "user_sessions_path": "data/user_sessions.csv"
        }
        genre_row_maker(**kwargs)  # 3 genre alts for every user
        logging.info("rerank SIDs for personalization")
        kwargs = {
            "model_path": arguments["--model"],
            "target_users_path": arguments.get("--target_users", None),
            "target_items_path": arguments.get("--target_items", None),
            "filter_items": arguments.get("--filter_items", None),
            "watched_list_rerank": arguments.get("--watched_list", None),
            "max_nb_reco": int(arguments["--max_nb_reco"]),
            "min_nb_reco": int(arguments["--min_nb_reco"]),
        }
        alt_reranker.alt_reranker(**kwargs).genre_rows(input_path="data/genre_rows.csv",
                                                       output_path=f'{arguments["<ALT_code>"]}-{arguments["<ALT_domain>"]}.csv')

    logging.info(f"execution time = {time.time() - start_time}")

if __name__ == '__main__':
    main()
