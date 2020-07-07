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
    with open(input_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().replace('"', '').split(",")
                nb = len(arr) - 1
                SIDs = arr[1:1 + int(nb / 3)]
                yield arr[0], list(set(SIDs))
            else:
                break


def video_byw(ALT_code, ALT_domain, filter_items_path=None, watched_list_rerank=None, min_nb_reco=4,
             user_sessions_path='data/new_user_sessions.csv', cbf_table_path="data/cbf_integration.csv"):
    """
    video-video similarity for because you watched(BYW)

    current logic:
    get all SIDs from user watch history
    ->  remove SIDs whose cbf_list is similar to others

    output: one line for one byw_sid, user may have several lines for each SID he watched
    user_multi_account_id,byw_sid,sakuhin_codes,alt_score
    user_multi_account_id,byw_sid,sakuhin_codes,alt_score


    :param cbf_table_path: sakuhin_public_code, rs_list
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

    logging.info("making because_you_watched rows for new session users")
    with open(f'{ALT_code}-{ALT_domain}.csv', "w") as w:
        # read userid & sids
        for line_counter, (userid, session_sids) in enumerate(new_user_session_reader(input_path=user_sessions_path)):
            if line_counter%10001 == 1:
                logging.info(f"{line_counter} lines done")
                
            # to record which session_sids are alive after removing similar SIDs.  SIDs:session_id
            session_dict = {cbf_dict[session_sid]:session_sid for session_sid in session_sids if cbf_dict.get(session_sid, None)}
            cbf_rs_lists = [k.split("|") for k, v in session_dict.items()]

            # check the similarity between cbf_rs_lists of SIDs
            similar_threshold = 0.5
            to_del = []
            while True:
                if len(cbf_rs_lists) <= 1:
                    break

                for i in range(len(cbf_rs_lists)-1):
                    for j in range(i+1, len(cbf_rs_lists)):
                        sa = set(cbf_rs_lists[i])
                        sb = set(cbf_rs_lists[j])
                        if max(len(sa & sb)/len(sa), len(sa & sb)/len(sb)) > similar_threshold:
                            to_del.append(cbf_rs_lists[j])
                    if to_del:
                        break
                else:
                    break

                if to_del:
                    for e in to_del:
                        cbf_rs_lists.remove(e)
                        del session_dict["|".join(e)]
                    to_del = []

            for SIDs, session_SID in session_dict.items():
                # do filtering
                arrs = [sid for sid in SIDs.split("|") if sid not in set(filter_items) and
                        sid not in set(dict_watched_sakuhin.get(userid, []))]
                # TODO: current order of SIDs is based on cbf scores, we can mix cbf score with user bpr score
                if len(arrs) >= min_nb_reco:
                    # user_multi_account_id,byw_sid,sakuhin_codes,alt_score
                    w.write('{},{},{},{:.4f}\n'.format(userid, session_SID, "|".join(arrs), 1.0))
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








