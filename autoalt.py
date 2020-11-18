"""autoalt

Usage:
    autoalt.py top <feature_public_code>  --input=PATH  [--blacklist=PATH --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py byw <feature_public_code>  [--blacklist=PATH  --watched_list=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py new_arrival <feature_public_code> [--input=PATH --model=PATH  --blacklist=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py allocate_FETs --input=PATH --output=PATH
    autoalt.py check_reco --input=PATH --blacklist=PATH [allow_blackSIDs]

Options:
    -h --help Show this screen
    --version
    --input PATH          File or dir path of input
    --output PATH         File or dir path of output
    --model PATH          File path location of the trained model
    --top_n=<tn>          Number of recommended items. [default: 10]
    --max_nb_reco=<nbr>   Maximal number of items in one ALT [default: 30]
    --min_nb_reco=<tn>    Minimal number of items in one ALT [default: 3]
    --nb_alt=<nbgr>       how many alts made for each user  [default: 3]
    feature_public_code   detail@dim_autoalt
    ALT_domain            SOUGOU, movie, book, manga, music … etc. SOUGOU means mixing all ALT_domain types together
    --blacklist PATH      filter_out_sakuhin_implicit.csv
    --watched_list PATH   watched_list_rerank.csv
    --target_users PATH   active users
    --target_items PATH   target items to recommend
    --series PATH         path of SID-series_id file


"""
"""
output of each ALT
file_name:<feature_public_code>-<ALT_domain>.csv 

format:  
* no head
* user_multi_account_id,SID|SID|...  , alt_score
* byw: user_multi_account_id,byw_sid,SID|SID|...  ,alt_score
* genre: user_multi_account_id,type-VARIETY-nations-日本,SID|SID|...,alt_score(from genre, not bpr)

"""
import os, logging, time
from datetime import date
import pandas as pd
from docopt import docopt
import yaml
from daily_top import DailyTop
from new_arrival import NewArrival
from because_you_watched import BecauseYouWatched
# from genre_row_maker import make_alt as genre_row_maker

logging.basicConfig(level=logging.INFO)

with open("config.yaml") as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)


def allocate_fets_to_alt_page(dir_path, output_path="feature_table.csv"):
    # allocate files in dir_path to feature_table.csv

    feature_table_writer = open(output_path, 'w')
    feature_table_writer.write(config['header']['feature_table'])

    for file in os.listdir(dir_path):

        if file == 'dim_autoalt.csv':
            logging.info(f'skip {file}')
            continue
        elif 'JFET' in file or 'CFET' in file or 'SAFET' in file:  # 自動生成ALTs
            logging.info(f'processing {file}')
            with open(os.path.join(dir_path, file), "r") as r:
                r.readline()
                while True:
                    line = r.readline()
                    if line:
                        feature_table_writer.write(line.rstrip() + ',2020-01-01 00:00:00,2029-12-31 23:59:59\n')
                    else:
                        break
        elif 'toppick' in file:  # TODO: this is workaround solution, eventually FET_toppick will be the same format as other 自動生成FETs
            logging.info(f'processing {file}')
            with open(os.path.join(dir_path, file), "r") as r:
                r.readline()
                while True:
                    line = r.readline()
                    if line:
                        arr = line.rstrip().split(',')
                        feature_table_writer.write(f'{arr[0]},JFET000001,{arr[-1]},{arr[2]},new あなたへのおすすめ,video,1,2020-01-01 00:00:00,2029-12-31 23:59:59\n')
                    else:
                        break
        else:  # 調達部FETs from alt_features_implicit.csv  TODO directly get this format from upstream
            logging.info(f'processing {file}')
            with open(os.path.join(dir_path, file), "r") as r:
                r.readline()
                while True:
                    line = r.readline()
                    if line:
                        arr = line.rstrip().split(',')
                        if len(arr) < 11:  # somehow some lines are empty
                            continue
                        elif len(arr) > 11:
                            arr[10] = ' '.join(arr[10:])  # for those lines w/ too more "," ->  join them

                        # don't save title info
                        # title = arr[9].rstrip().replace('"', '').replace("'", "").replace(',', '')
                        # description = arr[10].rstrip().replace('"', '').replace("'", "").replace(',', '')
                        if "semi_adult" in file:
                            feature_table_writer.write(f"{arr[0]},{arr[1]},{arr[2]},{arr[4]},,semi_adult,{arr[7]},{arr[5]},{arr[6]}\n")
                        else:  # ippan format:
                            feature_table_writer.write(f"{arr[0]},{arr[1]},{arr[2]},{arr[4]},,{arr[8]},{arr[7]},{arr[5]},{arr[6]}\n")
                    else:
                        break
    logging.info("feature_table.csv allocation done")


def check_reco(reco_path, blacklist_path, allow_blackSIDs=False):
    """
    override, since the reco format is different
    """
    blacklist = set()
    with open(blacklist_path, "r") as r:
        while True:
            line = r.readline()
            if line:
                blacklist.add(line.rstrip())
            else:
                break
    logging.info(f"{len(blacklist)} blacklist SIDs load")

    # user_multi_account_id,feature_public_code,create_date,sakuhin_codes,feature_title,domain,autoalt
    line_counter = 0
    with open(reco_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                line_counter += 1
                unique_sid_pool = set()
                SIDs = line.split(",")[3].split("|")

                for sid in SIDs:
                    if not sid:
                        raise Exception(f"[check_reco]: {reco_path} has a line [{line.rstrip()}] which has no SIDs")

                    if not allow_blackSIDs and sid in blacklist:
                        raise Exception(f"[black_list] {sid} in {line}")

                    if sid not in unique_sid_pool:
                        unique_sid_pool.add(sid)
                    else:
                        raise Exception(f"[duplicates] duplicated {sid}")
            else:
                break
    if allow_blackSIDs:
        logging.info(f"{reco_path} (w/ {line_counter} lines) skips blacklist and passes duplicates check, good to go")
    else:
        logging.info(f"{reco_path} (w/ {line_counter} lines) passes blacklist and duplicates check, good to go")


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    start_time = time.time()
    today = date.today().strftime("%Y%m%d")  # e.g. 20200915

    # read dim_autoalt.csv
    if any([arguments['top'], arguments['new_arrival'], arguments['byw']]):
        df = pd.read_csv("data/dim_autoalt.csv")
        alt_info = df[df['feature_public_code'] == arguments["<feature_public_code>"]]

        if len(alt_info) != 1:
            logging.error(f'found {len(alt_info)} alts w/ {arguments["<feature_public_code>"]} in dim_autoalt')
            return
        if arguments['top']:
            # python autoalt.py top CFET000001 --input data/daily_top_genre.csv --blacklist data/filter_out_sakuhin_implicit.csv  --max_nb_reco 30
            alt = DailyTop(alt_info, create_date=today, blacklist_path=arguments.get("--blacklist", None),
                           series_path=arguments["--series"],
                           max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(input_path=arguments["--input"])
        elif arguments["new_arrival"]:
            # python autoalt.py new_arrival JFET000003 --model data/implicit_bpr.model.2020-10-31  --blacklist data/filter_out_sakuhin_implicit.csv --series data/sid_series.csv
            alt = NewArrival(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                             series_path=arguments["--series"],
                             max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(input_path=arguments['--input'], bpr_model_path=arguments["--model"])
        elif arguments["byw"]:
            # python autoalt.py  byw JFET000002 --blacklist data/filter_out_sakuhin_implicit.csv  --watched_list data/watched_list_ippan.csv --series data/sid_series.csv
            alt = BecauseYouWatched(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                                    series_path=arguments["--series"],
                                    max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(arguments["--watched_list"])
        elif arguments['genre_row']:
            raise Exception("genre_row is invalid using current bad TAGs :(")
        else:
            raise Exception("Unimplemented ALT")

    elif arguments['allocate_FETs']:
        allocate_fets_to_alt_page(arguments['--input'], arguments['--output'])
    elif arguments['check_reco']:
        check_reco(arguments["--input"], arguments["--blacklist"], arguments['allow_blackSIDs'])
    else:
        raise Exception("Unimplemented ERROR")


    """
    if arguments['new_arrival']:
        # python altmaker.py new_arrival ALT_new_arrival video --model data/implicit_bpr.model.{?}  [--top_n=<tn> | --target_users=PATH]
        new_arrival_maker(alt_info, today, arguments['--model'], int(arguments["--min_nb_reco"]))
    elif arguments['top']:
        # python autoalt.py top CFET000001 --input data/daily_top_genre.csv --blacklist data/filter_out_sakuhin_implicit.csv  --max_nb_reco 30
        alt = DailyTop(alt_info, create_date=today, blacklist_path=arguments["--blacklist"], max_nb_reco=30)
        alt.make_alt(input_path=arguments["--input"])
        alt.check_reco(f"{alt_info['feature_public_code'].values[0]}.csv")
    elif arguments['byw']:
        kwargs = {
            "alt_info":alt_info,
            "create_date":today,
            "min_nb_reco": int(arguments["--min_nb_reco"]),
            "filter_items": arguments.get("--filter_items", None),
            "watched_list_ippan": arguments.get("--watched_list", None),
            "user_sessions_path": 'data/new_user_sessions.csv',
            "cbf_table_path": "data/postplay_implicit.csv"
        }
        byw_maker(**kwargs)
    elif arguments['allocate_FETs']:
        allocate_fets_to_alt_page('data/')
    elif arguments['genre_row']:
        # altmaker.py genre_row <feature_public_code> <ALT_domain> --model=PATH [--min_nb_reco=<tn> --nb_alt=<nbgr>  --max_nb_reco=<nbr> --target_users=PATH --target_items=PATH --filter_items=PATH  --watched_list=PATH]
        raise Exception("genre_row is invalid using current bad TAGs :(")
    """

    logging.info(f"execution time = {time.time() - start_time}")


if __name__ == '__main__':
    main()

"""
        # python altmaker.py genre_row ALT_genrerow video --model ../data/bpr/implicit_bpr.model.2020-06-06 --target_users data/target_users.csv
        kwargs = {
            "feature_public_code": arguments["<feature_public_code>"],
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
                                                       output_path=f'{arguments["<feature_public_code>"]}-{arguments["<ALT_domain>"]}.csv')
"""