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


def daily_top(top_N = 10, ALT_code="ALT_daily_top", ALT_domain="video",
              input_path="data/daily_top.csv"):
    if ALT_domain == "video":
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
        user_count = 0

        with open(f"{ALT_code}-{ALT_domain}.csv", "w") as w:
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
    elif ALT_domain == "book":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    start_time = time.time()

    if arguments['new_arrival']:
        # python altmaker.py new_arrival ALT_new_arrival video --model data/implicit_bpr.model.{?}  [--top_n=<tn> | --target_users=PATH]
        new_arrival_maker(arguments['--model'], ALT_code=arguments["<ALT_code>"], ALT_domain=arguments["<ALT_domain>"])
    elif arguments['top']:
        daily_top(top_N=int(arguments['--top_n']), input_path="data/daily_top.csv",
                  ALT_code=arguments["<ALT_code>"], ALT_domain=arguments["<ALT_domain>"])
    elif arguments['byw']:
        # python altmaker.py byw ALT_byw video --filter_items data/filter_out_sakuhin_implicit.csv --watched_list data/watched_list_rerank.csv --min_nb_reco 4
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
        # python altmaker.py genre_row ALT_genrerow video --model ../data/bpr/implicit_bpr.model.2020-06-06 --target_users data/target_users.csv
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
