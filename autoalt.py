"""autoalt

Usage:
    autoalt.py top <feature_public_code>  --input=PATH  [--blacklist=PATH --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py byw <feature_public_code> --sid_name=PATH --watched_list=PATH [--blacklist=PATH  --target_users=PATH --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py new_arrival <feature_public_code> [--input=PATH --model=PATH  --blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py trending <feature_public_code> --model=PATH --trending=PATH --toppick=PATH [--blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py popular <feature_public_code> --model=PATH --popular=PATH --already_reco=PATH [--blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py tag <feature_public_code> --model=PATH --watched_list=PATH --already_reco=PATH [--blacklist=PATH  --target_users=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH]
    autoalt.py toppick <feature_public_code> --model=PATH [--blacklist=PATH  --max_nb_reco=<tn> --min_nb_reco=<tn> --series=PATH --target_users=PATH --target_items=PATH]
    autoalt.py allocate_FETs --input=PATH --output=PATH [--target_users=PATH]
    autoalt.py allocate_FETs_page <feature_public_code> --input=PATH
    autoalt.py check_reco --input=PATH --blacklist=PATH [allow_blackSIDs]
    autoalt.py demo_candidates --input=PATH --output=PATH
    autoalt.py rm_series --input=PATH --output=PATH --series=PATH --target_users=PATH
    autoalt.py coldstart <feature_public_code> --input=PATH

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
    --sid_name PATH       path of SID-name file


"""
import os
import logging
import time
from datetime import date
import pandas as pd
from docopt import docopt
import yaml
from autoalts.daily_top import DailyTop
# from autoalts.toppick import TopPick
from autoalts.new_arrival import NewArrival
from autoalts.because_you_watched import BecauseYouWatched
from autoalts.trending import Trending
from autoalts.popular import Popular
from autoalts.tag_alt import TagAlt
from autoalts.coldstart import ColdStartExclusive
from autoalts.utils import make_demo_candidates, toppick_rm_series
from utils import efficient_reading

logging.basicConfig(level=logging.INFO)

with open("config.yaml") as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)


def allocate_fets_to_page_generation(feature_table_path, page_public_code):
    """
    for users which have AutoALTs Reco, allocate & rank their autoalts, for Page Generation

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

    user_fets_dict = {}  # {userid: [FET]}
    for line in efficient_reading(feature_table_path):
        arr = line.split(",")
        if len(arr) < 2:
            logging.info(f"WRONG : {line}")
        if 'JFET' in arr[1]:  # TODO: SFET
            user_fets_dict[arr[0]] = user_fets_dict.setdefault(arr[0], []) + [arr[1]]
    logging.info(f"read {feature_table_path} done")

    def fet_ranking(fet):
        return fet_score_dict[fet]

    with open(f"reco_{page_public_code}_autoalts_page.csv", "w") as w:
        w.write("user_multi_account_id,autoalts\n")
        for user_id, fets in user_fets_dict.items():
            unsorted_fets = user_fets_dict[user_id] + CFETs
            w.write(f"{user_id},{'|'.join(sorted(unsorted_fets, key=fet_ranking, reverse=True))}\n")


def allocate_fets_to_fet_table(dir_path, output_path="feature_table.csv", target_users_path=None):
    """
    allocate all feature files under dir_path

    output: feature_table.csv @ config['header']['feature_table']

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
    target_users = []
    if target_users_path:
        for line in efficient_reading(target_users_path):
            target_users.append(line.rstrip())
        logging.info(f"AutoALTs are for {len(target_users)} target users, chotatatus ALTs are still for all users")

    feature_table_writer = open(output_path, 'w')
    feature_table_writer.write(config['header']['feature_table'])

    for file in os.listdir(dir_path):
        logging.info(f"processing {file}")
        # define output convertion function based on input file format

        if 'CFET' in file:  # 自動生成ALTs
            def autoalt_format(line):
                return line
                # return line.rstrip() + ',2020-01-01 00:00:00,2029-12-31 23:59:59\n'
            output_func = autoalt_format
        elif 'JFET' in file:  # 自動生成ALTs & for target users only
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
        else:  #if file == 'dim_autoalt.csv' or file == "target_users.csv":
            logging.info(f'skip {file}')
            continue
        '''
        elif 'coldstart' in file:
            # choutatsu FETs from reco_ippan_choutatsu_coldstart_features.csv
            # format: user_multi_account_id,feature_public_code,sakuhin_codes,feature_score,feature_ranking,genre_tag_code,
            # platform,film_rating_order,feature_public_flg,feature_display_flg,feature_home_display_flg,
            # feature_public_start_datetime,feature_public_end_datetime,create_date
            if "choutatsu" in file:
                def coldstart_format(line):
                    arr = line.rstrip().split(",")
                    if "semiadult" in file:
                        return f'{arr[0]},{arr[1]},{arr[-1]},{arr[2]},,semiadult,0,{arr[-3]},{arr[-2]}\n'
                    elif "ippan" in file:
                        return f'{arr[0]},{arr[1]},{arr[-1]},{arr[2]},,ippan_sakuhin,0,{arr[-3]},{arr[-2]}\n'
            # toppick FETs from autoalt_toppick_osusume_only -> autoalt_coldstart_toppick_osusume_only.csv
            # format: user_multi_account_id,block,create_date,feature_name,sakuhin_codes,sub_text,score
            #elif "toppick" in file:  TODO
            #    logging.info("coldstart & toppick")
            #    def coldstart_format(line):
            #        arr = line.rstrip().split(",")
            #        return f"{arr[0]},JFET000001,{arr[2]},{arr[4]},あなたへのおすすめ,ippan_sakuhin,1,2020-01-01 00:00:00,2029-12-31 23:59:59\n"
            else:
                raise Exception("COLDSTART FILE ERROR")
            output_func = coldstart_format
        elif 'toppick' in file:
            def toppick_format(line):
                arr = line.rstrip().split(',')
                return f'{arr[0]},JFET000001,{arr[-1]},{arr[3]},あなたへのおすすめ,ippan_sakuhin,1,2020-01-01 00:00:00,2029-12-31 23:59:59\n'
            output_func = toppick_format
        '''
        existing_user_fets = {}  # {user_multi_account_id:feature_public_code}

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

                # check function
                '''
                arr = output_str.split(",")
                user_sids = existing_user_fets.get(arr[0],[])
                if arr[1] in user_sids:
                    logging.info(f"ERROR, {arr[0]} got two same FETs:{arr[1]}, current processing:{file}")
                    raise Exception("ERROR FOUND")
                else:
                    existing_user_fets[arr[0]] = user_sids + [arr[1]]
                '''
            else:
                logging.info(f"{file}:{line} WRONG FORMAT")

        feature_table_writer.flush()
        logging.info(f"{file} : {linecnt} lines processed")

    feature_table_writer.close()

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
    if any([arguments['top'], arguments['toppick'], arguments['new_arrival'], arguments['byw'], arguments["trending"],
            arguments["popular"], arguments['coldstart'], arguments['tag']]):
        df = pd.read_csv("data/dim_autoalt.csv")
        alt_info = df[df['feature_public_code'] == arguments["<feature_public_code>"]]

        if len(alt_info) != 1:
            logging.error(f'found {len(alt_info)} alts w/ {arguments["<feature_public_code>"]} in dim_autoalt')
            return
        if arguments['top']:
            # python autoalt.py top CFET000001 --input data/daily_top.csv --blacklist data/filter_out_sakuhin_implicit.csv  --max_nb_reco 30
            alt = DailyTop(alt_info, create_date=today, blacklist_path=arguments.get("--blacklist", None),
                           series_path=arguments["--series"],
                           max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(input_path=arguments["--input"])
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
            # python autoalt.py new_arrival JFET000003 --model data/implicit_bpr.model.2020-10-31  --blacklist data/filter_out_sakuhin_implicit.csv --series data/sid_series.csv
            alt = NewArrival(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                             series_path=arguments["--series"], target_users_path=arguments.get("--target_users", None),
                             max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(input_path=arguments['--input'], bpr_model_path=arguments["--model"])
        elif arguments["byw"]:
            # python autoalt.py  byw JFET000002 --blacklist data/filter_out_sakuhin_implicit.csv  --watched_list data/watched_list_ippan.csv --series data/sid_series.csv
            alt = BecauseYouWatched(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                                    series_path=arguments["--series"], sid_name_path=arguments["--sid_name"],
                                    target_users_path=arguments.get("--target_users", None),
                                    max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(arguments["--watched_list"])
        elif arguments["trending"]:
            # python autoalt.py trending JFET000005 --model data/implicit_bpr.model.2021-03-21 --trending data/trending.csv --toppick data/toppick.csv --blacklist data/filter_out_sakuhin_implicit.csv --target_users data/superusers.csv --series data/sid_series.csv
            alt = Trending(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                             series_path=arguments["--series"], target_users_path=arguments.get("--target_users", None),
                             max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(trending_path=arguments['--trending'], toppick_path=arguments['--toppick'], bpr_model_path=arguments["--model"])
        elif arguments["popular"]:
            # python autoalt.py popular JFET000004 --model data/implicit_bpr.model.2021-03-21 --popular data/popular.csv --already_reco data/already_reco_SIDs.csv --blacklist data/filter_out_sakuhin_implicit.csv --target_users data/superusers.csv --series data/sid_series.csv
            alt = Popular(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                             series_path=arguments["--series"], target_users_path=arguments.get("--target_users", None),
                             max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(popular_sids_path=arguments['--popular'], already_reco_path=arguments['--already_reco'],
                         bpr_model_path=arguments["--model"])
        elif arguments['tag']:
            # python autoalt.py tag JFET000006 --model data/implicit_bpr.model.2021-03-21 --watched_list data/user_sid_history_daily.csv  --already_reco data/already_reco_SIDs.csv   --blacklist data/filter_out_sakuhin_implicit.csv  --target_users data/superusers.csv  --series data/sid_series.csv
            alt = TagAlt(alt_info, create_date=today, blacklist_path=arguments["--blacklist"],
                             series_path=arguments["--series"], target_users_path=arguments.get("--target_users", None),
                             max_nb_reco=arguments['--max_nb_reco'], min_nb_reco=arguments["--min_nb_reco"])
            alt.make_alt(bpr_model_path=arguments["--model"], user_sid_history_path=arguments['--watched_list'])  # TODO: add others

        elif arguments['coldstart']:
            alt = ColdStartExclusive(alt_info, create_date=today)
            alt.make_alt(input=arguments["--input"])
        elif arguments['genre_row']:
            raise Exception("genre_row is invalid using current bad TAGs :(")
        else:
            raise Exception("Unimplemented ALT")

    elif arguments['allocate_FETs']:
        allocate_fets_to_fet_table(arguments['--input'], arguments['--output'], arguments.get("--target_users", None))
    elif arguments['allocate_FETs_page']:
        # python autoalt.py allocate_FETs_page MAINPAGE --input data/autoalt_ippan_sakuhin_features.csv
        allocate_fets_to_page_generation(arguments['--input'], arguments["<feature_public_code>"])
    elif arguments['check_reco']:
        check_reco(arguments["--input"], arguments["--blacklist"], arguments['allow_blackSIDs'])
    elif arguments['demo_candidates']:
        make_demo_candidates(feature_table_path=arguments['--input'], output_path=arguments['--output'])
    elif arguments['rm_series']:
        toppick_rm_series(series_path=arguments['--series'], input=arguments['--input'], output=arguments['--output'],
                          target_users_path=arguments['--target_users'])
    else:
        raise Exception("Unimplemented ERROR")

    logging.info(f"execution time = {time.time() - start_time}")


if __name__ == '__main__':
    main()
