"""altmaker

Usage:
    altmaker.py top <feature_public_code> [--top_n=<tn>]
    altmaker.py byw <feature_public_code> [--min_nb_reco=<tn> --filter_items=PATH  --watched_list=PATH]
    altmaker.py new_arrival <feature_public_code> --model=PATH [--min_nb_reco=<tn>]
    altmaker.py allocate_FETs

Options:
    -h --help Show this screen
    --version
    --model PATH          File path location of the trained model
    --top_n=<tn>          Number of recommended items. [default: 10]
    --max_nb_reco=<nbr>   Maximal number of items in one ALT [default: 30]
    --min_nb_reco=<tn>    Minimal number of items in one ALT [default: 3]
    --nb_alt=<nbgr>       how many alts made for each user  [default: 3]
    feature_public_code              detail@dim_autoalt
    ALT_domain            SOUGOU, movie, book, manga, music … etc. SOUGOU means mixing all ALT_domain types together
    --filter_items PATH   filter_out_sakuhin_implicit.csv
    --watched_list PATH   watched_list_rerank.csv
    --target_users PATH   active users
    --target_items PATH   target items to recommend


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
from new_arrival import make_alt as new_arrival_maker
from because_you_watched import make_alt as byw_maker
# from genre_row_maker import make_alt as genre_row_maker

logging.basicConfig(level=logging.INFO)


def daily_top(alt_info,  # as a dataframe
              create_date,
              top_N=10, input_path="data/daily_top.csv"):
    """
    logic:
    make ALT_daily_top by order of daily_top.csv

    :param alt_info:
    :param create_date:
    :param top_N:
    :param input_path:
    :return:
    """

    if alt_info['domain'].values[0] == "video":
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

        # just make one row for user "COMMON"
        with open(f"{alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write("user_multi_account_id,feature_public_code,create_date,sakuhin_codes,"
                    "feature_name,feature_description,domain,is_autoalt\n")
            w.write(f"COMMON,{alt_info['feature_public_code'].values[0]},{create_date},{SIDs_str},"
                    f"{alt_info['feature_name'].values[0]},,{alt_info['domain'].values[0]},1\n")

        """ 
        # this version make one row for each user
        user_count = 0
        with open(f"{feature_public_code}-{ALT_domain}.csv", "w") as w:
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
        """
    elif alt_info['domain'].values[0] == "book":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")


def daily_top_genre(alt_info,  # as a dataframe
              create_date,
              top_N=10, input_path="data/daily_top_genre.csv"):
    """
    logic:
    make ALT_daily_top by order and genre

    :param alt_info:
    :param create_date:
    :param top_N:
    :param input_path:
    :return:
    """

    if alt_info['domain'].values[0] == "video":
        SIDs = []  # keep SID unique
        genres = []

        with open("data/daily_top_genre.csv", 'r') as r:
            r.readline()  # skip the first line
            while True:
                line = r.readline()
                if line:  # ep,SID,nb_watch
                    arr = line.rstrip().replace('"', '').split(",")
                    SIDs.append(arr[1])
                    genres.append(arr[3])
                else:
                    break

        genre_dict = {}  # genre: SID list
        for SID, genre in zip(SIDs, genres):

            SID_list = genre_dict.get(genre, None)
            if not SID_list:
                genre_dict.setdefault(genre, [SID])
            elif SID not in SID_list:
                genre_dict[genre] = SID_list + [SID]

        max_len = 0  # longest length of
        for v in genre_dict.values():
            max_len = max(max_len, len(v))

        # make recommendation by order and genre
        for k in genre_dict.keys():
            genre_dict[k] = genre_dict[k] + [None] * (max_len - len(genre_dict[k]))

        reco = []
        for i in range(max_len):
            for k in genre_dict.keys():
                if genre_dict[k][i]:
                    reco.append(genre_dict[k][i])

        reco_str = '|'.join(reco)

        # just make one row for user "COMMON"
        with open(f"{alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write("user_multi_account_id,feature_public_code,create_date,sakuhin_codes,"
                    "feature_name,feature_description,domain,is_autoalt\n")
            w.write(f"COMMON,{alt_info['feature_public_code'].values[0]},{create_date},{reco_str},"
                    f"{alt_info['feature_name'].values[0]},,{alt_info['domain'].values[0]},1\n")

    elif alt_info['domain'].values[0] == "book":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")


def allocate_fets_to_alt_page(dir_path):
    # allocate files in dir_path to alt_table.csv

    alt_table_writer = open("alt_table.csv", 'w')
    alt_table_writer.write("user_multi_account_id,feature_public_code,create_date,sakuhin_codes,"
                           "feature_name,feature_description,domain,is_autoalt\n")

    for file in os.listdir(dir_path):

        if file == 'dim_autoalt.csv':
            logging.info(f'skip {file}')
            continue
        elif 'JFET' in file or 'CFET' in file:  # 自動生成ALTs
            logging.info(f'processing {file}')
            with open(os.path.join(dir_path, file), "r") as r:
                r.readline()
                while True:
                    line = r.readline()
                    if line:
                        alt_table_writer.write(line)
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

                        title = arr[9].rstrip().replace('"', '').replace("'", "").replace(',', '')
                        description = arr[10].rstrip().replace('"', '').replace("'", "").replace(',', '')
                        alt_table_writer.write(f"{arr[0]},{arr[1]},{arr[2]},{arr[4]},{title},{description},{arr[8]},{arr[7]}\n")
                    else:
                        break
    logging.info("alt_table.csv allocation done")


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    start_time = time.time()

    today = date.today().strftime("%Y%m%d")  # e.g. 20200915

    # read dim_autoalt.csv
    df = pd.read_csv("data/dim_autoalt.csv")
    alt_info = df[df['feature_public_code'] == arguments["<feature_public_code>"]]

    if not arguments['allocate_FETs'] and len(alt_info) != 1:
        logging.error(f'found {len(alt_info)} alts w/ {arguments["<feature_public_code>"]} in dim_autoalt')
        return

    if arguments['new_arrival']:
        # python altmaker.py new_arrival ALT_new_arrival video --model data/implicit_bpr.model.{?}  [--top_n=<tn> | --target_users=PATH]
        new_arrival_maker(alt_info, today, arguments['--model'], int(arguments["--min_nb_reco"]))
    elif arguments['top']:
        # python altmaker.py top JFET00001 --top_n 10
        # daily_top(alt_info, today, top_N=int(arguments['--top_n']), input_path="data/daily_top.csv")
        daily_top_genre(alt_info, today, top_N=int(arguments['--top_n']), input_path="data/daily_top_genre.csv")
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
    logging.info(f"execution time = {time.time() - start_time}")


if __name__ == '__main__':
    main()
