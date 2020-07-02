import logging
from dstools.utils import normalize_path, save_list_to_file, file_to_list


logger = logging.getLogger(__name__)


# past N days is corresponding to past N days of new_arrival_EP.csv
def new_user_session_reader(input_path="data/new_user_sessions.csv"):
    """
    input format:
    "user_id", "SIDs..." , "episode_public_codes...", "watch times"

    :return: a dict {user_id: { SID: episode_public_codes}}
    """
    userid_list, sid_list = [], []
    with open(input_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().replace('"', '').split(",")
                nb = len(arr) - 1
                SIDs = arr[1:1 + int(nb / 3)]
                userid_list.append(arr[0])
                sid_list.append(SIDs[-1])  # TODO: currently for lastest one only
            else:
                break
    return userid_list, sid_list


def video_byw(ALT_code, ALT_domain, filter_items_path=None, watched_list_rerank=None, min_nb_reco=4,
             user_sessions_path='data/new_user_sessions.csv', cbf_table_path="data/cbf_integration.csv"):
    """
    video-video similarity for because you watched(BYW)

    1st version: simplest logic, only for latest watched sakuhin

    :param user_sessions_path: userid, sids, watched_time
    :param cbf_table_path: sakuhin_public_code, rs_list
    :return:
    """
    # read content-based filtering recommendation
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
    userid_list, sid_list = new_user_session_reader(input_path=user_sessions_path)

    # read filtering items
    filter_items = []
    if filter_items:
        filter_items = file_to_list(filter_items_path)
    dict_watched_sakuhin = {}
    if watched_list_rerank:
        list_watched = file_to_list(watched_list_rerank)
        for i in list_watched:
            userid, sids = i.split(',')
            dict_watched_sakuhin[userid] = sids.split("|")
        del list_watched

    logging.info("making because_you_watched rows for {} new session users".format(len(userid_list)))
    with open(f'{ALT_code}-{ALT_domain}.csv', "w") as w:
        for userid, session_sid in zip(userid_list, sid_list):
            rs_list = cbf_dict.get(session_sid, None)

            if rs_list:
                # do filtering
                arrs = [sid for sid in rs_list.split("|") if sid not in set(filter_items) and
                                             sid not in set(dict_watched_sakuhin.get(userid, []))]

                # TODO: current order is based on cbf scores, we can mix cbf score with user bpr score
                if len(arrs) >= min_nb_reco:
                    # user_multi_account_id,byw_sid,sakuhin_codes,alt_score
                    w.write('{},{},{},{:.4f}\n'.format(userid, session_sid, "|".join(arrs), 1.0))
            else:
                pass


def make_alt(ALT_code, ALT_domain, filter_items=None, watched_list_rerank=None, min_nb_reco=10,
             user_sessions_path='data/new_user_sessions.csv', cbf_table_path="data/cbf_integration.csv"):
    if ALT_domain == "video_all":
        video_byw(ALT_code, ALT_domain, filter_items, watched_list_rerank, min_nb_reco,
             user_sessions_path, cbf_table_path)
    elif ALT_domain == "book_all":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")








