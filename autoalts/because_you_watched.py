import logging
from autoalts.autoalt_maker import AutoAltMaker

logging.basicConfig(level=logging.INFO)


class BecauseYouWatched(AutoAltMaker):
    def __init__(self, alt_info, create_date, blacklist_path, series_path=None, max_nb_reco=30, min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)

    def make_alt(self, watched_list_ippan=None):
        if self.alt_info['domain'].values[0] == "video":
            self.video_byw(watched_list_ippan)
        elif self.alt_info['domain'].values[0] == "semiadult":
            self.semiadult()
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    # past N days is corresponding to past N days of new_arrival_EP.csv
    def new_user_session_reader(self, input_path="data/new_user_sessions.csv"):
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
                    if arr[0] == '':  # userid == '' represent users w/o login
                        continue
                    nb = len(arr) - 1
                    SIDs = arr[1:1 + int(nb / 3)]
                    yield arr[0], list(set(SIDs))
                else:
                    break

    def semiadult(self, user_sessions_path='data/new_user_sessions.csv',
                  postplay_path="data/semiadult_postplay_implicit.csv",
                   sid_name_path="data/sid_name_dict.csv"):
        """
        logic: due to lack of data and 作品, don't use watched_list to filter out watched SIDs
        """
        logging.info("loading sid, name lookup table")
        sid_name_dict = {}
        with open(sid_name_path, "r") as r:
            r.readline()
            while True:
                line = r.readline()
                if line:
                    arr = line.rstrip().split(",")
                    sid_name_dict.setdefault(arr[0], arr[1])
                else:
                    break

        logging.info("loading postplay as ICF reco")
        icf_dict = {}
        with open(postplay_path, 'r') as r:  # SID, SID|SID|...
            while True:
                line = r.readline()
                if line:
                    arrs = line.rstrip().split(",")
                    cbf_recos = icf_dict.get(arrs[0], None)
                    if cbf_recos:
                        cbf_recos = cbf_recos.split("|")
                        postplay_recos = [sid for sid in arrs[1].split("|") if sid not in cbf_recos]
                        icf_dict[arrs[0]] = '|'.join(cbf_recos + postplay_recos)
                    else:
                        icf_dict.setdefault(arrs[0], arrs[1])
                else:
                    break
        logging.info(f"cbf table has {len(icf_dict)} items")

        logging.info("making because_you_watched rows for new session users")
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['autoalt'])

            for line_counter, (userid, session_sids) in enumerate(self.new_user_session_reader(input_path=user_sessions_path)):
                if line_counter%10000 == 1:
                    logging.info(f"{line_counter} lines done")

                # check the similarity between cbf_rs_lists of SIDs
                # to record which session_sids are alive after removing similar SIDs.  SIDs:session_id
                session_dict = {icf_dict[session_sid]: session_sid for session_sid in session_sids if
                                icf_dict.get(session_sid, None)}
                cbf_rs_lists = [k.split("|") for k, v in session_dict.items()]
                similar_threshold = 0.5
                to_del = []

                while True:
                    if len(cbf_rs_lists) <= 1:
                        break

                    for i in range(len(cbf_rs_lists) - 1):
                        for j in range(i + 1, len(cbf_rs_lists)):
                            sa = set(cbf_rs_lists[i])
                            sb = set(cbf_rs_lists[j])
                            if max(len(sa & sb) / len(sa), len(sa & sb) / len(sb)) > similar_threshold:
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
                    # blacklist filtering & watched SID filtering
                    arrs = [sid for sid in SIDs.split("|")]

                    # TODO: current order of SIDs is based on cbf scores, we can mix cbf score with user bpr score
                    if len(arrs) >= self.min_nb_reco:
                        title = sid_name_dict.get(session_SID, None)
                        if title:
                            title = title.rstrip().replace('"', '').replace("'", "")
                            title = self.alt_info['feature_title'].values[0].replace("○○", title)

                            w.write(f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(arrs)},"
                                    f"{title},{self.alt_info['domain'].values[0]},1\n")

                            # TODO: for MVP, we only make one BYW FET
                            break
                        else:
                            logging.warning(f"{session_SID} can not find a mapping title")
                else:
                    pass

    def video_byw(self, watched_list_ippan=None,
                  user_sessions_path='data/new_user_sessions.csv',
                  cbf_table_path="data/cbf_rs_list.csv",
                  postplay_path="data/postplay_implicit.csv"):
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
        # TODO: rerank by BPR, currrent workaround = no reranking <- may need A/B test

        logging.info("loading sid, name lookup table")
        sid_name_dict = {}
        with open("data/sid_name_dict.csv", "r") as r:
            r.readline()
            while True:
                line = r.readline()
                if line:
                    arr = line.rstrip().split(",")
                    sid_name_dict.setdefault(arr[0], arr[1])
                else:
                    break

        logging.info("loading content-based filtering recommendation")
        cbf_dict = {}
        with open(cbf_table_path, 'r') as r:  # SID, SID|SID|...
            while True:
                line = r.readline()
                if line:
                    arrs = line.rstrip().split(",")
                    cbf_dict.setdefault(arrs[0], arrs[1])
                else:
                    break

        logging.info("loading postplay to complement cbf")
        with open(postplay_path, 'r') as r:  # SID, SID|SID|...
            while True:
                line = r.readline()
                if line:
                    arrs = line.rstrip().split(",")
                    cbf_recos = cbf_dict.get(arrs[0], None)
                    if cbf_recos:
                        cbf_recos = cbf_recos.split("|")
                        postplay_recos = [sid for sid in arrs[1].split("|") if sid not in cbf_recos]
                        cbf_dict[arrs[0]] = '|'.join(cbf_recos+postplay_recos)
                    else:
                        cbf_dict.setdefault(arrs[0], arrs[1])
                else:
                    break
        logging.info(f"cbf table has {len(cbf_dict)} items")


        logging.info("loading watched_list_ippan as user seen items")
        # TODO maintain a seen list for speed up
        dict_watched_sakuhin = {}
        with open(watched_list_ippan, "r") as r:  # userid,item,rating ; rating != 1 -> bookmark
            while True:
                line = r.readline()
                if line:
                    arr = line.rstrip().split(",")
                    if arr[2] == '1':
                        userid = arr[0]
                        dict_watched_sakuhin[userid] = dict_watched_sakuhin.setdefault(userid, []) + [arr[1]]
                else:
                    break

        logging.info("making because_you_watched rows for new session users")
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['autoalt'])

            # read userid & sids
            for line_counter, (userid, session_sids) in enumerate(self.new_user_session_reader(input_path=user_sessions_path)):
                if line_counter%10000 == 1:
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
                    watched_SIDs = set(dict_watched_sakuhin.get(userid, []))
                    # blacklist filtering & watched SID filtering
                    arrs = [sid for sid in SIDs.split("|") if sid not in self.blacklist and
                            sid not in watched_SIDs]

                    arrs = self.rm_series(arrs)

                    # TODO: current order of SIDs is based on cbf scores, we can mix cbf score with user bpr score
                    if len(arrs) >= self.min_nb_reco:
                        title = sid_name_dict.get(session_SID, None)
                        if title:
                            title = title.rstrip().replace('"','').replace("'","")
                            title = self.alt_info['feature_title'].values[0].replace("○○", title)

                            w.write(f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(arrs)},"
                                    f"{title},{self.alt_info['domain'].values[0]},1\n")

                            # TODO: for MVP, we only make one BYW FET
                            break
                        else:
                            logging.warning(f"{session_SID} can not find a mapping title")
                else:
                    pass

