"""altmaker

Usage:
    altmaker.py top <alt_public_code> <alt_domain> --target_users=PATH
    altmaker.py byw <alt_public_code> <alt_domain> [--top_n=<tn>]
    altmaker.py new_arrival --model=PATH <alt_public_code> <alt_domain> [--top_n=<tn> | --target_users=PATH]
    altmaker.py genre_row --model=PATH <alt_public_code> <alt_domain> [--top_n=<tn> | --target_users=PATH]

Options:
    -h --help Show this screen
    --version
    --model PATH         File path location of the trained model
    --top_n=<tn>         Number of recommended items. [default: 10]
    alt_public_code      detail@dim_alt
    alt_domain           SOUGOU, movie, book, manga, music … etc. SOUGOU means mixing all ALT_domain types together

"""
import os, logging
from docopt import docopt
from dstools.logging import setup_logging
from dstools.cli.parser import parse
from dstools.utils import normalize_path, save_list_to_file, file_to_list
import alt_reranker
from genre_row_maker import worker as genre_row_maker
from new_arrival import make_alt as new_arrival_maker

setup_logging()
logger = logging.getLogger(__name__)


# TODO: workaround solution here, should have smart way to do it, since it is same for every user
def daily_top(opts, alt_public_code="ATL_daily_top", alt_genre="SOUGOU",
              input_path="data/daily_top.csv", output_path="daily_top_processed.csv"):
    assert opts.get("target_users", None), "Wrong, need target_users!!"
    _, target_users, _ = alt_reranker.read_target_user(opts.get("target_users", None))

    SIDs = []
    with open(input_path, 'r') as r:
        r.readline()  # skip the first line
        while True:
            line = r.readline()
            if line:
                SIDs.append(line.rstrip().split(",")[0])
            else:
                break
    SIDs = '|'.join(SIDs)
    logging.info('Daily Top in UNEXT = {} for {} target users'.format(SIDs, len(target_users)))
    with open(output_path, "w") as w:
        w.write("user_multi_account_id,alt_public_code,alt_genre,sakuhin_codes,alt_score\n")
        for uid in target_users:
            w.write("{},{},{},{},{:.4f}\n".format(uid, alt_public_code, alt_genre, SIDs, 1.0))


def byw_rows(opts, user_sessions_path='data/new_user_sessions.csv', cbf_table_path="data/cbf_integration.csv"
             , output_path="byw_rows.csv"):
    """
    video-video similarity for because you watched(BYW)

    1st version: simplest logic, only for latest watched sakuhin

    :param opts:
    :param user_sessions_path: userid, sids, watched_time
    :param cbf_table_path: sakuhin_public_code, rs_list
    :return:
    """
    cbf_dict = {}
    with open(cbf_table_path, 'r') as r:
        while True:
            line = r.readline()
            if line:
                arrs = line.rstrip().split(",")
                cbf_dict.setdefault(arrs[0], arrs[1])
            else:
                break

    # read userid & sids
    userid_list, sid_list = [], []
    with open(user_sessions_path, 'r') as r:
        r.readline()  # skip 1st line
        while True:
            # format: userid, sids, watched_time
            # userid -> "PMXXX" -> have to remove "
            # sid -> "SIDxxx, ... SIDyyy" -> have to remove " , same as watched_time
            line = r.readline()
            if line:
                arrs = line.rstrip().replace('"', '').split(",")
                userid_list.append(arrs[0])
                latest_one = arrs[1].split(",")[0]
                sid_list.append(latest_one)
            else:
                break

    # read filtering items
    filter_items = []
    if opts.get("filter_items", None):
        filter_items = file_to_list(opts.get("filter_items", None))
    dict_watched_sakuhin = {}
    if opts.get("watched_list_rerank", None):
        list_watched = file_to_list(opts.get("watched_list_rerank", None))
        for i in list_watched:
            userid, sids = i.split(',')
            dict_watched_sakuhin[userid] = sids.split("|")
        del list_watched

    logging.info("making because you watched rows for {} new session users".format(len(userid_list)))
    with open(output_path, "w") as w:
        w.write("user_multi_account_id,alt_public_code,alt_genre,sakuhin_codes,alt_score\n")
        for userid, session_sid in zip(userid_list, sid_list):
            rs_list = cbf_dict.get(session_sid, None)

            if rs_list:
                # do filtering
                arrs = [sid for sid in rs_list.split("|") if sid not in set(filter_items) and
                                             sid not in set(dict_watched_sakuhin.get(userid, []))]
                if len(arrs) >= 4:  # TODO
                    # user_multi_account_id,alt_public_code,alt_genre,sakuhin_codes,alt_score
                    logging.info('{},{},{},{},{:.4f}'.format(userid, 'ALT_BYW', 'SOUGOU', "|".join(arrs), 1.0))
                    w.write('{},{},{},{},{:.4f}\n'.format(userid, 'ALT_BYW', 'SOUGOU', "|".join(arrs), 1.0))
            else:
                pass




# TODO
"""
for quick development:
* limit 300 @ new_user_sessions.sql

"""
def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)
    cmd, opts = parse(arguments)
    #logger.info(f"Executing '{cmd}' with arguments {opts}")

    opts.update({
        # "model": "../ippan_verification/implicit_bpr.model.2020-02-23",
        "nb_reco": 30,  # 50 for all,  20 for genre
        # "target_users": "data/target_users.csv",
        "filter_items": "data/filter_out_sakuhin_implicit.csv",
        "watched_list_rerank": "data/watched_list_rerank.csv"
    })
    if arguments['new_arrival']:
        new_arrival_maker(arguments['--model'], alt_public_code=arguments["<alt_public_code>"], alt_domain=arguments["<alt_domain>"])
        # altmaker.py (new_arrival) --model=PATH <alt_public_code> <alt_genre> [--top_n=<tn> | --target_users=PATH]
        # alt_reranker.alt_reranker(opts).new_arrival(input_path="data/new_arrival.csv", alt_public_code=arguments["<alt_public_code>"], alt_genre=arguments["<alt_genre>"])
    elif arguments['top']:
        daily_top(opts, input_path="data/daily_top.csv",
                  alt_public_code=arguments["<alt_public_code>"], alt_genre=arguments["<alt_genre>"])
    elif arguments['byw']:
        byw_rows(opts)
    elif arguments['genre_row']:
        genre_row_maker(nb_genre_row=3, nb_neco=opts['nb_reco'])  # 3 genre alts for every user
        alt_reranker.alt_reranker(opts).genre_rows(input_path="data/genre_rows.csv",
                                                   output_path="genre_rows_reranked.csv")
        # TODO
        """
        performance checking: takes around 1:46 
2020-04-24 19:53:52,791 - root - INFO - using model for 1515250 users and 23175 items
2020-04-24 19:56:02,477 - root - INFO - type-DRAMA-nations-アメリカ done, 1 lines done with 13022 lines written
2020-04-24 19:56:09,888 - root - INFO - type-DRAMA-genre-action done, 2 lines done with 15118 lines written
2020-04-24 19:57:24,238 - root - INFO - nations-アメリカ-genre-action done, 3 lines done with 21914 lines written
2020-04-24 20:18:31,469 - root - INFO - type-ANIME-nations-日本 done, 4 lines done with 136432 lines written
2020-04-24 20:19:51,404 - root - INFO - type-ANIME-tag-劇場版アニメ（国内） done, 5 lines done with 147403 lines written
2020-04-24 20:20:29,739 - root - INFO - type-ANIME-genre-mystery done, 6 lines done with 158296 lines written
...
2020-04-24 21:39:44,176 - root - INFO - nations-オーストラリア-genre-mystery done, 204 lines done with 690362 lines written
2020-04-24 21:39:44,238 - root - INFO - type-VARIETY-tag-釣り done, 205 lines done with 690362 lines written
2020-04-24 21:39:44,296 - root - INFO - type-VARIETY-tag-教養・語学 done, 206 lines done with 690362 lines written
-----------
around 2:40 
2020-04-27 22:06:20,621 - root - INFO - using model for 1515250 users and 23175 items
2020-04-27 22:06:33,557 - root - INFO - type-DRAMA-genre-romance ing, 0 lines done with 0 lines written
2020-04-27 22:16:29,040 - root - INFO - nations-韓国-genre-romance ing, 1 lines done with 53852 lines written
...
2020-04-28 00:45:31,020 - root - INFO - type-VARIETY-tag-グラビア ing, 202 lines done with 1022057 lines written
2020-04-28 00:45:31,121 - root - INFO - type-VARIETY-tag-釣り ing, 203 lines done with 1022060 lines written
2020-04-28 00:45:31,185 - root - INFO - type-VARIETY-tag-スポーツ・競技 ing, 204 lines done with 1022060 lines written
2020-04-28 00:45:31,245 - root - INFO - type-VARIETY-tag-教養・語学 ing, 205 lines done with 1022060 lines written
2020-04-28 00:45:31,304 - root - INFO - nations-カナダ-genre-action ing, 206 lines done with 1022060 lines written
2020-04-28 00:45:31,365 - root - INFO - type-MUSIC_IDOL-tag-TBSオンデマンド ing, 207 lines done with 1022063 lines written
        """

if __name__ == '__main__':
    main()
