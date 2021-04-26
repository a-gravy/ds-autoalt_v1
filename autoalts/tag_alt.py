import os, sys
import logging
import pandas as pd
import numpy as np
from collections import Counter, defaultdict
from autoalts.autoalt_maker import AutoAltMaker
import plyvel

PROJECT_PATH = os.path.abspath("%s/.." % os.path.dirname(__file__))
sys.path.append(PROJECT_PATH)
# from bpr.implicit_recommender import rank
from utils import efficient_reading, rank

logger = logging.getLogger(__name__)


class UserProfling():
    def __init__(self, userid):
        self.score_threshold = 0.01
        self.userid = userid
        self.nation_bucket = []
        self.genre_bucket = []
        self.type_bucket = []
        # self.tag_bucket = []

        self.type_name_converting = {
            "YOUGA": "洋画",
            "HOUGA": "邦画",
            "FDRAMA": "海外ドラマ",
            "ADRAMA": "韓流・アジアドラマ",
            "DRAMA": "国内ドラマ",
            "ANIME": "アニメ",
            "KIDS": "キッズ",
            "DOCUMENT": "ドキュメンタリー",
            "MUSIC_IDOL": "音楽・アイドル",
            "SPORT": "スポーツ",
            "VARIETY": "バラエティ"
        }

        self.cant_combo_types = ["YOUGA", "HOUGA", "FDRAMA", "ADRAMA", "DRAMA"]

    def score_processing(self, score):  # 6.91665654355 -> 6.91
        return int(score * 100) / float(100)

    def add(self, tag, tag_score, type_set, genre_set, nation_set):
        tag_score = self.score_processing(tag_score)

        if tag in type_set:
            if tag == "SEMIADULT":
                return
            else:
                bucket = self.type_bucket
        elif tag in nation_set:
            bucket = self.nation_bucket
        else:
            bucket = self.genre_bucket

        bucket.append((tag, tag_score))

    def show_info(self):
        print(f"user {self.userid}")
        print("nation: ", self.nation_bucket)
        print("genre: ", self.genre_bucket)
        print("type: ", self.type_bucket)

    def alt_combo_maker(self):
        # Rule-base version
        combo_query = []
        combo_name = []

        # 1. nation - genre
        if self.nation_bucket and self.genre_bucket:
            nation_name = self.nation_bucket.pop(0)[0]
            genre_name = self.genre_bucket.pop(0)[0]
            combo_name.append(self.alt_naming('nation+genre', **{'nation':nation_name, 'genre':genre_name}))
            combo_query.append([nation_name, genre_name])

        # TODO: special case for HOUGA, YOUGA, DRAMA, ADRAMA, FDRAMA
        # 2. genre - type
        if self.genre_bucket and self.type_bucket:
            genre_name = self.genre_bucket.pop(0)[0]
            type_name = self.type_bucket.pop(0)[0]
            combo_name.append(self.alt_naming('type+genre', **{'type':self.type_name_converting[type_name], 'genre':genre_name}))
            combo_query.append([genre_name, type_name])

        # 3. type
        if self.type_bucket:
            type_name = self.type_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("type", **{'type':self.type_name_converting[type_name]}))
            combo_query.append([type_name])

        # 4. nation
        if self.nation_bucket:
            nation_name = self.nation_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("nation", **{'nation': nation_name}))
            combo_query.append([nation_name])

        # 5. genre
        if self.genre_bucket:
            genre_name = self.genre_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("genre", **{'genre': genre_name}))
            combo_query.append([genre_name])

        return combo_query, combo_name
        #for query, name in zip(combo_query, combo_name):
        #    yield query, name

    def alt_naming(self, combo, **kwargs):
        """
        :param combo: e.g. nation:日本-genre:action / genre:action-type:anime  / type:anime
        :return:
        """
        if combo == "nation+type":
            return f"{kwargs['type']}/{kwargs['nation']}のおすすめ"
        elif combo == "nation+genre":
            return f"{kwargs['nation']}/{kwargs['genre']}"
        elif combo == "type+genre":
            return f"{kwargs['type']}/{kwargs['genre']}"
        elif combo == "nation":
            return f"製作国:{kwargs['nation']}のピックアップ"
        elif combo == "type":
            return f"{kwargs['type']}のおすすめ"
        elif combo == "genre":
            return f"{kwargs['genre']}のおすすめ"
        else:
            raise Exception(f"WRONG COMBO {combo}")


class TagAlt(AutoAltMaker):
    def __init__(self, alt_info, create_date, target_users_path, blacklist_path, series_path=None, max_nb_reco=20,
                 min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)
        self.target_users = None
        if target_users_path:
            self.target_users = self.read_target_users(target_users_path)

        # self.types = [s.lower() for s in mapping.types]
        self.inverted_index = None
        self.sid_vector = {}  # {sid: [0, 1, 1, 0, ...]}
        self.type_set = set()
        self.genre_set = set()
        self.nation_set = set()

        if os.path.exists('user_profiling_tags'):
            logging.info("previous user_profiling_tags exists, loading Tags...")
            self.leveldb = plyvel.DB('user_profiling_tags/', create_if_missing=False)
            tags = self.leveldb.get('tags'.encode('utf-8'), b'').decode("utf-8")
            self.tag_index_dict = {k: v for v, k in enumerate(tags.split('|'))}  # e.g. {'日本': 0}
        else:
            logging.info("no user_profiling_tags, building from scratch")
            self.leveldb = plyvel.DB('user_profiling_tags/', create_if_missing=True)
            self.tag_index_dict = {}  # e.g. {'日本': 0}

        if os.path.exists('JFET000006'):
            logging.info("previous JFET000006 leveldb exists, loading it")
            self.JFET000006 = plyvel.DB('JFET000006/', create_if_missing=False)
        else:
            logging.info("no JFET000006 leveldb, building from scratch")
            self.JFET000006 = plyvel.DB('JFET000006/', create_if_missing=True)


    def make_alt(self, bpr_model_path, user_sid_history_path,
                 already_reco_path='data/already_reco_SIDs.csv', sakuhin_meta_path='data/sakuhin_meta.csv',
                 lookup_table_path="data/sakuhin_lookup_table.csv"):
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.build_lookup(lookup_table_path)

            if not self.tag_index_dict:
                self.make_tag_index_dict(sakuhin_meta_path)

            # update levelDB JFET000006 & user_profiling_tags
            self.ippan_sakuhin(bpr_model_path, sakuhin_meta_path, user_sid_history_path, already_reco_path)

            # make JFET000006.csv based on levelDB JFET000006
            self.output_from_levelDB()

        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "adult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def make_tag_index_dict(self, sakuhin_meta_path):
        """
        go through all tags of all 作品, make index of Tag:

        output:
        * self.tag_index_dict:  {tag : index of tag}
        * save as levelDB tags: tag|tag|tag ...

        """
        logging.info("[Phase]- browse all tags to build a vector for all tags >= 100 counts")
        tags_cnt = Counter()

        # count all tags
        for line in efficient_reading(sakuhin_meta_path, True):
            sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
            self.type_set.add(sid_type)
            self.genre_set.update(sid_genres)
            self.nation_set.update(sid_nations)

            for x in [sid_type] + list(sid_genres) + list(sid_nations):
                tags_cnt[x] = tags_cnt[x] + 1

        # build {tag:index} for vector
        del tags_cnt['""']  # remove empty string
        for index, (tag, cnt) in enumerate(tags_cnt.most_common()):
            if cnt < 100:  # discard tags whose cnt < 100
                break
            self.tag_index_dict[tag] = index

        logging.info(f"got {len(self.tag_index_dict)} Tags")
        self.leveldb.put('tags'.encode('utf-8'), '|'.join(self.tag_index_dict.keys()).encode('utf-8'))

    def ippan_sakuhin(self, bpr_model_path, sakuhin_meta_path, user_sid_history_path, already_reco_path):
        tag_index_list = list(self.tag_index_dict.keys())
        print('tag_index_list = ', tag_index_list)

        logging.info("[Phase] - build tag index vector for all SIDs")
        # build sid_vector for all SIDs
        for line in efficient_reading(sakuhin_meta_path, True):
            v = np.zeros(len(self.tag_index_dict))
            sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
            for x in [sid_type] + list(sid_genres) + list(sid_nations):
                tag_index = self.tag_index_dict.get(x, None)
                if tag_index:
                    v[tag_index] = 1
            self.sid_vector[line.split(",")[0]] = v

        self.read_already_reco_sids(already_reco_path)

        model = self.load_model(bpr_model_path)

        # for bucket categorization
        for line in efficient_reading(sakuhin_meta_path, True):
            sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
            self.type_set.add(sid_type)
            self.genre_set.update(sid_genres)
            self.nation_set.update(sid_nations)

        logging.info("[Phase] - making Tag ALTs, updating levelDB user_profiling_tags & levelDB JFET000006")
        nb_reco_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for line in efficient_reading(user_sid_history_path, True):
                userid = line.split(",")[0]

                if self.target_users and userid not in self.target_users:
                    continue

                # read previous user_profiling_vector from leveldb
                user_profiling_vector = self.leveldb.get(userid.encode('utf-8'), b'').decode("utf-8")
                if user_profiling_vector:
                    # string -> np.array
                    user_profiling_vector = np.array(user_profiling_vector.split('|'), dtype=float)
                else:
                    user_profiling_vector = np.zeros(len(self.tag_index_dict))

                # build user vector based on history (from old -> new)
                for sid in line.rstrip().split(",")[1].split("|")[::-1]:
                    sid_vector = self.sid_vector.get(sid, [])
                    if sid_vector != []:
                        user_profiling_vector = user_profiling_vector * 0.9 + sid_vector

                # update leveldb: np.array -> string
                self.leveldb.put(userid.encode('utf-8'),
                       '|'.join(['{:.3f}'.format(x) for x in user_profiling_vector]).encode('utf-8'))

                # build Tag combo based on user profile
                user_profiling = UserProfling(userid)
                for index_tag in (np.argsort(-user_profiling_vector))[:20]:
                    if user_profiling_vector[index_tag] < 0.01:
                        break
                    user_profiling.add(tag_index_list[index_tag], user_profiling_vector[index_tag],
                                       self.type_set, self.genre_set, self.nation_set)

                # get personalized SID ranking
                bpr_sid_list = None
                for userid, sid_list, score_list in rank(model, target_users=[userid],
                                                           filter_already_liked_items=True, top_n=-1, batch_size=10000):
                    bpr_sid_list = sid_list

                if not bpr_sid_list:
                    logging.debug(f"user:{userid} has no data in the model")
                    continue

                bpr_sid_set = set(bpr_sid_list)

                # prevent duplicates
                # read toppick[:5], trending[:5], popular[:5] as default, then update every Tag ALT
                already_reco_sids = self.already_reco_dict.get(userid, set())
                # output_list = [ alt_name1:SIDa|SIDb|SIDc ]
                output_list = []
                combo_queries, combo_names = user_profiling.alt_combo_maker()

                for combo_query, combo_name in zip(combo_queries, combo_names): #user_profiling.alt_combo_maker():
                    sids = self.do_query(combo_query)
                    sids = self.black_list_filtering(sids)
                    sids = self.rm_series(sids)
                    # sids = list(sids)
                    if len(sids) < self.min_nb_reco:
                        logging.debug(f"{combo_name} got too less SIDs")
                        continue
                    alt_sids = [sids[index] for index in np.argsort(
                        [bpr_sid_list.index(sid) for sid in sids if sid in bpr_sid_set and sid not in already_reco_sids])][:20]
                    already_reco_sids.update(alt_sids[:5])

                    output_list.append(f"{combo_name}={'|'.join(alt_sids[:self.max_nb_reco])}")

                    if len(output_list) == 3:    # TODO 3 only
                        break
                if output_list:
                    self.JFET000006.put(userid.encode('utf-8'), '+'.join(output_list).encode('utf-8'))

                    # TODO: rm
                    #for i in range(len(output_list)):
                    #    w.write(f"{userid},{self.alt_info['feature_public_code'].values[0]}_{i + 1},{output_list[i]}\n")

                nb_reco_users += 1
                if nb_reco_users % 1000 == 1:
                    logging.info(f"{nb_reco_users} users got processed")

        logging.info("{} users got reco / total nb of target user: {}, coverage rate={:.3f}".format(nb_reco_users, len(self.target_users),
                                                                                             nb_reco_users / len(self.target_users)))

    def ippan_sakuhin_from_scratch(self, bpr_model_path, sakuhin_meta_path, user_sid_history_path):
        logging.info("Phase 1 - browse all tags to build a vector for all tags >= 100 counts")
        tags_cnt = Counter()
        # count all tags
        for line in efficient_reading(sakuhin_meta_path, True):
            for tag in self.tags_preprocessing(line):
                tags_cnt[tag] = tags_cnt[tag] + 1

        # build {tag:index} for vector
        for index, (tag, cnt) in enumerate(tags_cnt.most_common()):
            if cnt < 100:  # discard tags whose cnt < 100
                break
            self.tag_index_dict[tag] = index
        tag_index_list = list(self.tag_index_dict.keys())
        logging.info(f"got {len(self.tag_index_dict)} Tags")
        self.leveldb.put('tags'.encode('utf-8'), '|'.join(self.tag_index_dict.keys()).encode('utf-8'))

        logging.info("Phase 2 - build tag index vector for all SIDs")
        # build sid_vector for all SIDs
        for line in efficient_reading(sakuhin_meta_path, True):
            v = np.zeros(len(self.tag_index_dict))
            for tag in self.tags_preprocessing(line):
                if tag in tag_index_list:
                    v[self.tag_index_dict[tag]] = 1
            self.sid_vector[line.split(",")[0]] = v

        model = self.load_model(bpr_model_path)

        logging.info("Phase 3 - making Tag ALTs")
        nb_reco_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for line in efficient_reading(user_sid_history_path, True):
                userid = line.split(",")[0]

                if self.target_users and userid not in self.target_users:
                    continue

                user_profiling_vector = np.zeros(len(self.tag_index_dict))
                # build user vector based on history (from old -> new)
                for sid in line.rstrip().split(",")[1].split("|")[::-1]:
                    sid_vector = self.sid_vector.get(sid, [])
                    if sid_vector != []:
                        user_profiling_vector = user_profiling_vector * 0.9 + sid_vector

                self.leveldb.put(userid.encode('utf-8'),
                       '|'.join(['{:.3f}'.format(x) for x in user_profiling_vector]).encode('utf-8'))

                # build Tag combo based on user profile
                user_profiling = UserProfling(userid)
                for index_tag in (np.argsort(-user_profiling_vector))[:20]:
                    if user_profiling_vector[index_tag] < 0.01:
                        break
                    user_profiling.add(tag_index_list[index_tag], user_profiling_vector[index_tag])

                # get personalized SID ranking
                bpr_sid_list = None
                for userid, sid_list, score_list in rank(model, target_users=[userid],
                                                           filter_already_liked_items=True, top_n=-1, batch_size=10000):
                    bpr_sid_list = sid_list

                if not bpr_sid_list:
                    logging.debug(f"user:{userid} has no data in the model")
                    continue

                bpr_sid_set = set(bpr_sid_list)

                already_reco_sids = set()
                nb_reco_users += 1

                nb_alts_made = 0
                for combo_query, combo_name in user_profiling.alt_combo_maker(self.lookup):
                    # part_1 = combo.split('-')[0].split(':')
                    # part_2 = combo.split('-')[1].split(':')
                    # condition = (self.lookup[part_1[0]] == part_1[1]) & (self.lookup[part_2[0]] == part_2[1])
                    sids = self.do_query(combo_query)
                    sids = self.black_list_filtering(sids)
                    sids = self.rm_series(sids)
                    if not sids:
                        logging.debug(f"{combo_name} got 0 SIDs")
                        continue
                    alt_sids = [sids[index] for index in np.argsort(
                        [bpr_sid_list.index(sid) for sid in sids if sid in bpr_sid_set and sid not in already_reco_sids])][:20]
                    already_reco_sids.update(alt_sids[:10])

                    # JFET000006_1, JFET000006_2, ...
                    w.write(
                        f"{userid},{self.alt_info['feature_public_code'].values[0]}_{nb_alts_made+1},{self.create_date},{'|'.join(alt_sids[:self.max_nb_reco])},"
                        f"{combo_name},{self.alt_info['domain'].values[0]},1,"
                        f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")
                    nb_alts_made += 1
                    if nb_alts_made == 3:    # TODO 3 only
                        break

        logging.info("{} users got reco / total nb of target user: {}, coverage rate={:.3f}".format(nb_reco_users, len(self.target_users),
                                                                                             nb_reco_users / len(self.target_users)))

    def tags_preprocessing(self, line):
        # sakuhin_public_code,display_name,main_genre_code,menu_names,parent_menu_names,nations
        arr = line.rstrip().split(",")

        # preprocess for main_genre_code
        sid_type = arr[2]  # .lower()

        # preprocess for menu_names & parent_menu_names
        sid_genres = set()
        for menu_name in arr[3].split("|"):  # + arr[4].split("|"):
            sid_genres.add(menu_name)

        # preprocess for nations
        sid_nations = []
        if arr[5] != '':
            sid_nations = arr[5].split("/")

        return sid_type, sid_genres, set(sid_nations)

    def build_lookup(self, lookup_table_path):
        self.inverted_index = defaultdict(set)  # {'key': set( SIDs )}

        for line in efficient_reading(lookup_table_path, True):
            arr = line.rstrip().split(",")
            SID = arr[0]
            self.inverted_index[arr[2]].add(SID)  # main_genre_code
            self.inverted_index[arr[3]].add(SID)  # menu_name
            self.inverted_index[arr[5]].add(SID)  # nation
        logging.info("ALT lookup table built")

    def do_query(self, condition):
        query_sids = set()
        for key_word in condition:
            if not query_sids:
                query_sids = self.inverted_index[key_word]
            else:
                query_sids = query_sids & self.inverted_index[key_word]
        return query_sids  # set of SIDs

    def output_from_levelDB(self):
        logging.info("output to JFET000006.csv from levelDB JFET000006")
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for key, value in self.JFET000006:
                userid = key.decode('utf-8')
                output_list = value.decode("utf-8")
                if output_list:
                    for i, output_str in enumerate(output_list.split("+")):
                        output_arr = output_str.split("=")
                        combo_name = output_arr[0]
                        w.write(
                            f"{userid},{self.alt_info['feature_public_code'].values[0]}_{i + 1},{self.create_date},{output_arr[1]},"
                            f"{combo_name},{self.alt_info['domain'].values[0]},1,"
                            f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

