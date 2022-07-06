"""autoalt

Usage:
    autoalt.py top <feature_public_code> --env=<env> --input=PATH  [--blacklist=PATH --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py byw <feature_public_code> --env=<env> --sid_name=PATH --watched_list=PATH  --postplay=PATH [--blacklist=PATH  --target_users=PATH --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py new_arrival <feature_public_code> --env=<env> [--input=PATH --model=PATH  --blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py tag <feature_public_code> --env=<env> --model=PATH --watched_list=PATH [--reco_record=PATH --blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH  --batch_size=<bs>]
    autoalt.py new_arrival_sids <feature_public_code> --env=<env> --pool_path=PATH --user_profiling_path=PATH [pbar --watched_list=PATH  --reco_record=PATH --blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH  --batch_size=<bs>]
    autoalt.py (trending | popular | exclusives) <feature_public_code> --env=<env> --model=PATH --pool_path=PATH [--reco_record=PATH --blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH  --batch_size=<bs>]
    autoalt.py toppick <feature_public_code> --model=PATH  [--blacklist=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH --target_users=PATH --target_items=PATH]
    autoalt.py allocate_FETs --input=PATH --output=PATH [--target_users=PATH]
    autoalt.py allocate_FETs_page <feature_public_code> --input=PATH
    autoalt.py check_reco --input=PATH --target_users=PATH --blacklist=PATH
    autoalt.py demo_candidates --input=PATH --output=PATH
    autoalt.py rm_series --input=PATH --output=PATH --series=PATH --target_users=PATH
    autoalt.py coldstart <feature_public_code> --input=PATH
    autoalt.py make_filtering_matrix --model=PATH  --input=PATH --output=PATH

Options:
    -h --help Show this screen
    --version
    --input PATH          File or dir path of input
    --output PATH         File or dir path of output
    --model PATH          File path location of the trained model
    --top_n=<tn>          Number of recommended items. [default: 10]
    --max_nb_reco=<nbr>   Maximal number of items in one ALT [default: 20]
    --min_nb_reco=<tn>    Minimal number of items in one ALT [default: 4]
    --nb_alt=<nbgr>       how many alts made for each user  [default: 3]
    --batch_size=<bs>     the batch size of matrix operation [default: 1000]
    feature_public_code   detail@dim_autoalt
    ALT_domain            SOUGOU, movie, book, manga, music … etc. SOUGOU means mixing all ALT_domain types together
    --blacklist PATH      filter_out_sakuhin_implicit.csv
    --watched_list PATH   watched_list_rerank.csv
    --target_users PATH   active users
    --target_items PATH   target items to recommend
    --series PATH         path of SID-series_id file
    --sid_name PATH       path of SID-name file
    --pool_path PATH      path of SID pool file


"""
import os
import sys
import logging
import time
from datetime import date
import pandas as pd
from docopt import docopt
import tqdm
import yaml
from collections import defaultdict
from autoalts.daily_top import DailyTop
# from autoalts.toppick import TopPick
# from autoalts.basic_alt import BasicALT
from autoalts.popular import Popular
from autoalts.trending import Trending
from autoalts.new_arrival import NewArrival
from autoalts.new_arrival_sids import NewArrivalSIDs
from autoalts.because_you_watched import BecauseYouWatched
from autoalts.tag_alt import TagAlt
from autoalts.exclusives import Exclusives
from autoalts.coldstart import ColdStartExclusive
from autoalts.utils import make_demo_candidates, toppick_rm_series, efficient_reading
from filtering_matrix import make_filtering_matrix
import feature_table_checking

PROJECT_PATH = os.path.abspath("%s/.." % os.path.dirname(__file__))
sys.path.append(PROJECT_PATH)
from autoalts.utils import get_files_from_cloud, unzip_files_in_dir

logging.basicConfig(level=logging.INFO)

with open("config.yaml") as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)


def allocate_fets_to_page_generation(feature_table_path, page_public_code):
    """
    for users which have AutoALTs Reco, allocate & rank their autoalts by ALT score,
    for Page Generation

    e.g.
    userA,FET0012
    userA,FET0005
    userA,JFET0002
    userA,JFET0001

    ->userA,CFET0001|JFET0001|JFET0002


    :param feature_table_path: file made by allocate_fets_to_alt_page
    :param page_public_code: e.g. MAINPAGE
    :return:
    """
    fet_score_dict = {}
    CFETs = []
    for line in efficient_reading("data/dim_autoalt.csv"):
        arr = line.split(",")
        if arr[2] == page_public_code:
            fet_score_dict[arr[0]] = float(arr[3])
            if 'CFET' in arr[0]:
                CFETs.append(arr[0])

    if CFETs:
        logging.info(f"for PAGE:{page_public_code}, there are common FET:{CFETs} and \n {fet_score_dict}")

    user_fets_dict = defaultdict(list)  # {userid: [FET]}
    for line in efficient_reading(feature_table_path):
        arr = line.split(",")
        if len(arr) < 2:
            logging.info(f"WRONG : {line}")
        if 'JFET' in arr[1] and arr[1] not in user_fets_dict[arr[0]]:  # TODO: SFET
            user_fets_dict[arr[0]] = user_fets_dict[arr[0]] + [arr[1]]
            # user_fets_dict[arr[0]] = user_fets_dict.setdefault(arr[0], []) + [arr[1]]
    logging.info(f"read {feature_table_path} done")

    def fet_ranking(fet):
        return fet_score_dict[fet]

    with open(f"reco_{page_public_code}_autoalts_page.csv", "w") as w:
        w.write("user_multi_account_id,autoalts\n")
        for user_id, fets in user_fets_dict.items():
            unsorted_fets = user_fets_dict[user_id] + CFETs
            w.write(f"{user_id},{'|'.join(sorted(unsorted_fets, key=fet_ranking, reverse=True))}\n")


def feature_table_allocation(dir_path, output_path="feature_table.csv", target_users_path=None):
    """
    allocate all feature files under dir_path into one single file with uniform format
    for uploading to Cassandra

    output: feature_table.csv @ config['header']['feature_table']
    user_multi_account_id,feature_public_code,create_date,sakuhin_codes,feature_title,domain,autoalt,feature_public_start_datetime,feature_public_end_datetime

    input format: 調達部's header are different, but format is the same;  coldstart format is the same
    [ippan]
        調達部:
        user_multi_account_id,feature_public_code,create_date,feature_home_display_flg,sakuhin_codes,feature_public_start_datetime,feature_public_end_datetime,autoalt,domain,feature_title,feature_description

        coldstart:
        user_multi_account_id,feature_public_code,sakuhin_codes,feature_score,feature_ranking,genre_tag_code,platform,film_rating_order,feature_public_flg,feature_display_flg,feature_home_display_flg,feature_public_start_datetime,feature_public_end_datetime,create_date

    [semiadult]
        調達部:
        user_multi_account_id,alt_public_code,create_date,feature_home_display_flg,sakuhin_codes,feature_public_start_datetime,feature_public_end_datetime,autoalt,domain,alt_title,alt_description

        coldstart:
        user_multi_account_id,feature_public_code,sakuhin_codes,feature_score,feature_ranking,genre_tag_code,platform,film_rating_order,feature_public_flg,feature_display_flg,feature_home_display_flg,feature_public_start_datetime,feature_public_end_datetime,create_date

    :param dir_path:
    :param output_path:
    :return:
    """
    target_users = set()
    if target_users_path:
        for line in efficient_reading(target_users_path):
            target_users.add(line.rstrip())
        logging.info(f"AutoALTs are for {len(target_users)} target users, chotatatus ALTs are still for all users")

    feature_table_writer = open(output_path, 'w')
    feature_table_writer.write(config['header']['feature_table'])

    for file in os.listdir(dir_path):
        start_time = time.time()
        # define output convertion function based on input file format

        if 'CFET' in file:  # 自動生成ALTs
            def autoalt_format(line):
                return line
                # return line.rstrip() + ',2020-01-01 00:00:00,2029-12-31 23:59:59\n'
            output_func = autoalt_format
        elif 'JFET' in file:  # for target users only
            def autoalt_format(line):
                if target_users and line.split(",")[0] not in target_users:
                    return "not target user"
                else:
                    return line
                    # return line.rstrip() + ',2020-01-01 00:00:00,2029-12-31 23:59:59\n'
            output_func = autoalt_format
        elif "choutatsu" in file and 'coldstart' not in file:  # choutatsu
            def choutatsu_format(line):
                arr = line.rstrip().split(',')
                if len(arr) < 9:  # somehow some lines are empty
                    return "WRONG FORMAT"
                elif len(arr) > 9:
                    arr[8] = ' '.join(arr[8:])  # for those lines w/ too more "," ->  join them

                # don't save title info
                # title = arr[9].rstrip().replace('"', '').replace("'", "").replace(',', '')
                # description = arr[10].rstrip().replace('"', '').replace("'", "").replace(',', '')
                if "semiadult" in file:
                    return f"{arr[0]},{arr[1]},{arr[3]},{arr[2]},,semiadult,{arr[4]},{arr[6]},{arr[7]}\n"
                elif "ippan" in file:  # TODO, current ippan is ippan_sakuhin
                    return f"{arr[0]},{arr[1]},{arr[2]},{arr[3]},,ippan_sakuhin,{arr[6]},{arr[4]},{arr[5]}\n"

            output_func = choutatsu_format
        elif 'coldstart_features' in file:
            """
            reco_ippan_coldstart_features.csv
            
            input format: 
            user_multi_account_id,feature_public_code,sakuhin_codes,create_date,domain,
            autoalt,feature_title,feature_public_start_datetime,feature_public_end_datetime
            
            return: 
            logged-user-coldstart/non-logged-user-coldstart, feature_public_code, create_date, sakuhin_codes, ..., 
            feature_public_start_datetime,feature_public_end_datetime 
            """
            today = date.today().strftime("%Y%m%d")  # e.g. 20200915
            def coldstart_format(line):
                arr = line.rstrip().split(",")
                # one line for logged-user-coldstart
                # one line for non-logged-user-coldstart
                feature_title = ''  # don't save title info
                domain = 'ippan_sakuhin'
                autoalt = 0
                return f'coldstart,{arr[1]},{today},{arr[2]},{feature_title},{domain},{autoalt},{arr[-2]},{arr[-1]}\n' \
                       f'non-logged-in-coldstart,{arr[1]},{today},{arr[2]},{feature_title},{domain},{autoalt},{arr[-2]},{arr[-1]}\n'
            output_func = coldstart_format
        else:  #if file == 'dim_autoalt.csv' or file == "target_users.csv":
            logging.info(f'skip {file}')
            continue

        linecnt = 0
        for line in efficient_reading(os.path.join(dir_path, file)):
            output_str = output_func(line)
            if output_str == "WRONG FORMAT":
                logging.info(f"{file}:{line} WRONG FORMAT")
            elif output_str == "not target user":
                pass
            elif output_str:
                feature_table_writer.write(output_str)
                linecnt += 1
            else:
                logging.info(f"{file}:{line} WRONG FORMAT")

        feature_table_writer.flush()
        logging.info(f"{file} : {linecnt} lines processed")
        logging.info("takes {:.3f} sec".format(time.time() - start_time))

    feature_table_writer.close()

    logging.info("feature_table.csv allocation done")


def check_path_existing(**kwargs):
    for key, file_path in kwargs.items():
        if os.path.exists(file_path):
            pass
        else:
            raise Exception(f"[Error] {key}'s {file_path} doesn't exist")


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    start_time = time.time()
    today = date.today().strftime("%Y%m%d")  # e.g. 20200915

    if any([arguments['top'], arguments['toppick'], arguments['new_arrival'], arguments['new_arrival_sids'], arguments['byw'], arguments["trending"],
            arguments["popular"], arguments['coldstart'], arguments['tag'], arguments['exclusives']]):

        # at first, read dim_autoalt.csv
        get_files_from_cloud(env=arguments["--env"], **{'':"data/dim_autoalt.csv"})
        logging.info("Unzipping files")
        unzip_files_in_dir("data/")

        df = pd.read_csv("data/dim_autoalt.csv")
        alt_info = df[df['feature_public_code'] == arguments["<feature_public_code>"]]

        # basic args of creating an autoalt instance allocation
        basic_kwarg = {
            "alt_info":alt_info,
            "create_date":today,
            "blacklist_path":arguments.get("--blacklist", None),
            "series_path":arguments["--series"],
            "max_nb_reco":arguments['--max_nb_reco'],
            "min_nb_reco":arguments["--min_nb_reco"],
            "env": arguments["--env"]
        }

        if len(alt_info) != 1:
            logging.error(f'can not find {len(alt_info)} alts w/ {arguments["<feature_public_code>"]} in dim_autoalt')
            return

        alt_func = None
        kwargs = None

        if arguments['top']:
            # python autoalt.py top CFET000001 --input data/daily_top.csv --blacklist data/black_list.csv  --max_nb_reco 10
            kwargs = {
                'input_path': arguments["--input"],
            }
            kwargs.update(basic_kwarg)
            alt_func = DailyTop

        elif arguments['toppick']:
            pass
            """
            alt = TopPick(alt_info, create_date=today, blacklist_path=arguments.get("--blacklist", None),
                           series_path=arguments["--series"],
                           max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(bpr_model_path=arguments["--model"], target_users_path=arguments['--target_users'],
                         target_items_path=arguments['--target_items'])
            """
        elif arguments["new_arrival"]:
            # python autoalt.py new_arrival JFET000003 --model data/implicit_bpr.model.2020-10-31  --blacklist data/black_list.csv --series data/sid_series.csv
            kwargs = {
                'target_users_path': arguments.get('--target_users', None),
                # below are for alt.make_alt()
                'input_path': arguments["--input"],
                'bpr_model_path':arguments['--model'],
                'new_arrival_EP_path':"data/new_arrival_ep.csv",
                'user_ep_history_path':"data/user_ep_history.csv",
            }
            kwargs.update(basic_kwarg)
            alt_func = NewArrival
        elif arguments["new_arrival_sids"]:
            # python autoalt.py  new_arrival_sids  JFET000008  --pool_path data/new_arrival_SIDs.csv --sakuhin_meta data/sakuhin_meta.csv --toppick data/toppick.csv  --blacklist data/black_list.csv --series data/sid_series.csv --target_users data/superusers.csv  --reco_record data/recorecord.pkl
            kwargs = {
                'target_users_path': arguments.get('--target_users', None),
                'watched_list_path':arguments["--watched_list"],
                # below are for alt.make_alt()
                # 'sakuhin_meta': arguments["--sakuhin_meta"],
                'pool_path': arguments["--pool_path"],
                'record_path': arguments.get("--reco_record", None),
                'batch_size': arguments["--batch_size"],
                'user_profiling_path': arguments["--user_profiling_path"],
                'pbar': arguments['pbar']
            }
            kwargs.update(basic_kwarg)
            alt_func = NewArrivalSIDs
        elif arguments["byw"]:
            # python autoalt.py  byw JFET000002 --sid_name data/sid_name_dict.csv --blacklist data/black_list.csv  --watched_list data/user_sid_history.csv  --postplay data/postplay_implicit.2021-05-17.csv  --series data/sid_series.csv --target_users data/superusers.csv
            kwargs = {
                'target_users_path': arguments.get('--target_users', None),
                'sid_name_path':arguments["--sid_name"],
                # below are for alt.make_alt()
                'watched_list_path':arguments["--watched_list"],
                'cbf_rs_list_path':"data/cbf_rs_list.csv",
                'postplay_path':arguments["--postplay"],
            }
            kwargs.update(basic_kwarg)
            alt_func = BecauseYouWatched
        elif arguments["trending"] or arguments["popular"]:
            kwargs = {
                'target_users_path': arguments.get('--target_users', None),
                # below are for alt.make_alt()
                'pool_path': arguments["--pool_path"],
                'record_path': arguments.get("--reco_record", None),
                'model_path': arguments["--model"],
                'batch_size': arguments["--batch_size"]
            }
            kwargs.update(basic_kwarg)
            if arguments["trending"]:
                alt_func = Trending
            elif arguments["popular"]:
                alt_func = Popular
        elif arguments['tag']:
            # python autoalt.py tag JFET000006 --model data/als_model.latest --watched_list data/user_sid_history_daily.csv   --reco_record data/recorecord.pkl    --blacklist data/black_list.csv  --target_users data/demo_users.csv  --series data/sid_series.csv
            # the user history data used here is userID, SID|SID|SID ... , different format from dscollab
            kwargs = {
                "target_users_path":arguments.get("--target_users", None),
                "cast_info_path":"data/cast_info.csv",
                # below are for alt.make_alt()
                'model_path': arguments["--model"],
                'user_sid_history_path': arguments['--watched_list'],
                'record_path':arguments.get("--reco_record", None),
                'sakuhin_meta_path': 'data/sakuhin_meta.csv',
                'lookup_table_path': "data/sakuhin_lookup_table.csv",
                "sakuhin_cast_path": "data/sakuhin_cast.csv",
                'batch_size': arguments["--batch_size"]
            }
            kwargs.update(basic_kwarg)
            alt_func = TagAlt
        elif arguments['exclusives']:
            # python autoalt.py exclusives JFET000007 --model data/als_model.latest --pool_path data/exclusive_sakuhins.csv --blacklist data/black_list.csv --series data/sid_series.csv --target_users data/demo_users.csv --reco_record data/recorecord.pkl
            kwargs = {
                'target_users_path': arguments.get('--target_users', None),
                # below are for alt.make_alt()
                'pool_path': arguments["--pool_path"],
                'record_path': arguments.get("--reco_record", None),
                'model_path': arguments["--model"],
                'batch_size': arguments["--batch_size"]
            }
            kwargs.update(basic_kwarg)
            alt_func = Exclusives
        elif arguments['coldstart']:
            alt = ColdStartExclusive(alt_info, create_date=today)
            alt.make_alt(input=arguments["--input"])
        else:
            raise Exception("Unimplemented ALT")

        # download all files in kwarg to data/
        # get_files_from_s3(domain_name=alt_info['domain'].values[0], **kwargs)
        logging.info(f"basic_kwarg = {kwargs}")
        get_files_from_cloud(**kwargs)
        # unzip
        logging.info("Unzipping files")
        unzip_files_in_dir("data/")

        alt = alt_func(**kwargs)
        alt.make_alt(**kwargs)

    elif arguments['allocate_FETs']:
        feature_table_allocation(arguments['--input'], arguments['--output'], arguments.get("--target_users", None))
        # TODO
        # check_fets(arguments['--output'], "data/black_list.csv", allow_blackSIDs=False)
    elif arguments['allocate_FETs_page']:
        # python autoalt.py allocate_FETs_page MAINPAGE --input data/autoalt_ippan_sakuhin_features.csv
        allocate_fets_to_page_generation(arguments['--input'], arguments["<feature_public_code>"])
    elif arguments['check_reco']:
        # python autoalt.py check_reco --input JFET000006.csv --blacklist data/black_list.csv
        feature_table_checking.black_empty_duplicates_checking(arguments["--input"], arguments["--target_users"],
                                        arguments.get("--blacklist", None))
    elif arguments['demo_candidates']:
        make_demo_candidates(feature_table_path=arguments['--input'], output_path=arguments['--output'])
    elif arguments['rm_series']:
        toppick_rm_series(series_path=arguments['--series'], input=arguments['--input'], output=arguments['--output'],
                          target_users_path=arguments['--target_users'])
    elif arguments['make_filtering_matrix']:
        make_filtering_matrix(model_path=arguments['--model'],
                              interaction_history_file=arguments['--input'],
                              output_model_path=arguments['--output'])
    else:
        raise Exception("Unimplemented ERROR")

    logging.info(f"execution time = {time.time() - start_time}")


if __name__ == '__main__':
    main()
