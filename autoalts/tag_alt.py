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


type_name_converting = {
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
    "VARIETY": "バラエティ",
}


def type_naming_converter(name):
    if name in type_name_converting:
        return type_name_converting[name]
    else:
        return name


class UserProfling():
    def __init__(self, userid):
        self.score_threshold = 0.01
        self.userid = userid
        self.nation_bucket = []
        self.genre_bucket = []
        self.type_bucket = []
        self.person_bucket = []
        # self.tag_bucket = []  # all other tags are called genre

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
        elif tag in genre_set:
            bucket = self.genre_bucket
        else:
            bucket = self.person_bucket

        bucket.append((tag, tag_score))

    def show_info(self):
        print(f"user {self.userid}")
        print("nation: ", self.nation_bucket)
        print("genre: ", self.genre_bucket)
        print("type: ", self.type_bucket)
        print("person: ", self.person_bucket)

    def alt_combo_maker(self, person_meta_dict, sakuhin_query_table):
        # Rule-base version
        combo_query = []
        combo_name = []

        # 1. nation - genre
        if self.nation_bucket and self.genre_bucket:
            nation_name = self.nation_bucket.pop(0)[0]
            genre_name = self.genre_bucket.pop(0)[0]
            combo_name.append(self.alt_naming('nation+genre', **{'nation':nation_name, 'genre':genre_name}))
            # combo_sids.append(sakuhin_query_table.do_query([nation_name, genre_name]))
            combo_query.append([nation_name, genre_name])

        # TODO: special case for HOUGA, YOUGA, DRAMA, ADRAMA, FDRAMA
        # 2. genre - type
        if self.genre_bucket and self.type_bucket:
            genre_name = self.genre_bucket.pop(0)[0]
            type_name = self.type_bucket.pop(0)[0]
            combo_name.append(self.alt_naming('type+genre', **{'type':type_naming_converter[type_name], 'genre':genre_name}))
            combo_query.append([genre_name, type_name])
            # combo_sids.append(sakuhin_query_table.do_query([genre_name, type_name]))

        # 3. type
        if self.type_bucket:
            type_name = self.type_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("type", **{'type':type_naming_converter[type_name]}))
            combo_query.append([type_name])
            # combo_sids.append(sakuhin_query_table.do_query([type_name]))

        # 4. person
        if self.person_bucket:
            while self.person_bucket:
                person_name_id = self.person_bucket.pop(0)[0]
                person = person_meta_dict.get(person_name_id, None)
                if person:
                    combo_name.append(self.alt_naming("person", **{'person': person.person_name}))
                if len(person.sids) < 4:
                    continue
                else:
                    combo_query.append(person.sids)
                    break

        # 5. nation
        if self.nation_bucket:
            nation_name = self.nation_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("nation", **{'nation': nation_name}))
            combo_query.append([nation_name])

        # 6. genre
        if self.genre_bucket:
            genre_name = self.genre_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("genre", **{'genre': genre_name}))
            combo_query.append([genre_name])

        # return combo_sids, combo_name
        for query, name in zip(combo_query, combo_name):
            if len(query) > 3:  # special case person.sids: don't need query
                combo_sids = query
            else:
                combo_sids = sakuhin_query_table.do_query(query)
            yield combo_sids, name

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
        elif combo == "person":
            return f"{kwargs['person']}のおすすめ"
        else:
            raise Exception(f"WRONG COMBO {combo}")


class PersonMeta:
    def __init__(self, person_name, sids, roles):
        # self.person_name_id = None
        self.person_name = person_name
        self.sids = sids
        role = roles.split("|")
        # TODO tmp
        self.role = role[0]

    def add_sid(self, sids):
        self.sids = sids


class SIDQueryTable:
    """
    build a inverted_index table for querying SIDs by conditions

    self.inverted_index =
    * {'tag': set( SIDs )}
    * {'tag+tag': set( SIDs )} <- auto adding while querying a new combo
    """
    def __init__(self, lookup_table_path):
        self.inverted_index = defaultdict(set)  # {'one tag': set( SIDs )}
        for line in efficient_reading(lookup_table_path, True):
            arr = line.rstrip().split(",")
            SID = arr[0]
            self.inverted_index[type_naming_converter(arr[-4])].add(SID)  # main_genre_code
            self.inverted_index[arr[3]].add(SID)  # menu_name
            self.inverted_index[arr[5]].add(SID)  # nation
        logging.info("ALT lookup table built")

    def do_query(self, condition):
        if len(condition) == 1:  # only one tag query
            return self.inverted_index[condition[0]]
        else:
            combo = f"{condition[0]}+{condition[1]}"  # support most 2 tags
            if combo in self.inverted_index:
                return self.inverted_index[combo]
            else:
                query_sids = set()
                for key_word in condition:
                    if not query_sids:
                        query_sids = self.inverted_index[key_word]
                    else:
                        query_sids = query_sids & self.inverted_index[key_word]
                self.inverted_index[combo] = query_sids  # add new query result

                return query_sids  # set of SIDs


class TagAlt(AutoAltMaker):
    def __init__(self, **kwargs):
        super().__init__(kwargs["alt_info"], kwargs["create_date"], kwargs["blacklist_path"], kwargs["series_path"],
                         kwargs["max_nb_reco"], kwargs["min_nb_reco"])

        self.path_params = {}

        self.target_users = None
        if kwargs["target_users_path"]:
            self.target_users = self.read_target_users(kwargs["target_users_path"])

        # self.types = [s.lower() for s in mapping.types]

        self.sid_vector = {}  # {sid: [0, 1, 1, 0, ...]}
        self.type_set = set()
        self.genre_set = set()
        self.nation_set = set()
        self.sid_cast_dict = {}  # sid: [person_name_id],  for concat w/ tags
        self.sakuhin_query_table = None

        # build person meta
        self.person_meta_dict = {}
        for line in efficient_reading(kwargs["cast_info_path"]):
            arr = line.rstrip().split(",")
            self.person_meta_dict[arr[0]] = PersonMeta(person_name=arr[1], sids=arr[2].split("|"), roles=arr[3])
        logging.info(f"we have {len(self.person_meta_dict)} person w/ meta")

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

    def make_alt(self, **kwargs):
        # (self, bpr_model_path, user_sid_history_path, already_reco_path, sakuhin_meta_path, lookup_table_path, sakuhin_cast_path):
        self.path_params = kwargs

        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.sakuhin_query_table = SIDQueryTable(self.path_params["lookup_table_path"])

            if not self.tag_index_dict:
                self.make_tag_index_dict()

            # update levelDB JFET000006 & user_profiling_tags
            self.ippan_sakuhin()

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

    def make_tag_index_dict(self,):
        """
        go through all tags of all 作品, make index of Tag:

        output:
        * self.tag_index_dict:  {tag : index of tag}
        * save as levelDB tags: tag|tag|tag ...

        """
        logging.info("[Phase]- browse all tags to build a vector for all tags >= 100 counts")
        tags_cnt = Counter()

        # count all tags
        for line in efficient_reading(self.path_params["sakuhin_meta_path"], True):
            sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
            self.type_set.add(sid_type)
            self.genre_set.update(sid_genres)
            self.nation_set.update(sid_nations)

            for x in [sid_type] + list(sid_genres) + list(sid_nations):
                tags_cnt[x] = tags_cnt[x] + 1

        # count cast
        for line in efficient_reading(self.path_params["sakuhin_cast_path"]):
            arr = line.rstrip().replace('"', '').split(",")
            sids = arr[2].split("|")
            self.sid_cast_dict[arr[1]] = sids
            for person_name_id in sids:  # [person_name_id]
                tags_cnt[person_name_id] = tags_cnt[person_name_id] + 1

        # build {tag:index} for vector
        del tags_cnt['""']  # remove empty string
        for index, (tag, cnt) in enumerate(tags_cnt.most_common()):
            if cnt < 5:  # discard tags whose cnt < 100 -> for cast cnt should be < 5?
                break
            self.tag_index_dict[self.txt_processing(tag)] = index
        self.leveldb.put('tags'.encode('utf-8'), '|'.join(self.tag_index_dict.keys()).encode('utf-8'))

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
        elif combo == "person":
            return f"{kwargs['person']}のおすすめ"
        else:
            raise Exception(f"WRONG COMBO {combo}")

    def query_sid_pool(self, query, already_reco_sids=None):
        combo_sids = self.sakuhin_query_table.do_query(query)
        combo_sids = set(combo_sids)
        combo_sids = self.black_list_filtering(combo_sids)
        combo_sids = self.rm_series(combo_sids)

        if already_reco_sids:
            combo_sids = set(combo_sids) - already_reco_sids

        if len(combo_sids) < self.min_nb_reco:
            return None
        else:
            return list(combo_sids)

    def rank_sid_pool(self, combo_sid_pool, ranked_sid_index_dict):
        return [combo_sid_pool[index] for index in np.argsort([ranked_sid_index_dict[sid] for sid in combo_sid_pool
                                                               if sid in ranked_sid_index_dict])][:self.max_nb_reco]

    def make_alt_combo_A_B(self, bucket_a, bucket_b, ranked_sid_index_dict, already_reco_sids):
        if bucket_a and bucket_b:
            for i in range(len(bucket_a)):
                name_a = bucket_a[i][0]  # get '歴史・時代劇' from ('歴史・時代劇', 4.91)
                for j in range(len(bucket_b)):
                    name_b = bucket_b[j][0]

                    # get a pool of SID candidates
                    combo_sid_pool = self.query_sid_pool([name_a, name_b], already_reco_sids)

                    if not combo_sid_pool:  # discard combo w/ < self.min_nb_reco
                        continue
                    else:
                        # rank sid
                        alt_sids = self.rank_sid_pool(combo_sid_pool, ranked_sid_index_dict)
                        already_reco_sids.update(alt_sids[:5])
                        bucket_a.pop(i)
                        bucket_b.pop(j)

                        return alt_sids, [name_a, name_b]
        return None, None

    def make_alt_combo_A(self, bucket_a, ranked_sid_index_dict, already_reco_sids):
        if bucket_a:
            for i in range(len(bucket_a)):
                name_a = bucket_a[i][0]  # get '歴史・時代劇' from ('歴史・時代劇', 4.91)
                # get a pool of SID candidates
                combo_sid_pool = self.query_sid_pool([name_a], already_reco_sids)

                if not combo_sid_pool:  # discard combo w/ < self.min_nb_reco
                    continue
                else:
                    # rank sid
                    alt_sids = self.rank_sid_pool(combo_sid_pool, ranked_sid_index_dict)
                    already_reco_sids.update(alt_sids[:5])
                    bucket_a.pop(i)

                    return alt_sids, name_a
        return None, None

    def alt_combo_maker(self, user_profiling, ranked_sid_index_dict):
        # Rule-base version
        alt_combo_sids_list = []
        combo_name_list = []
        already_reco_sids = self.already_reco_dict.get(user_profiling.userid, set())

        # 1. nation - genre
        alt_combo_sids , combo_names = self.make_alt_combo_A_B(user_profiling.nation_bucket, user_profiling.genre_bucket, ranked_sid_index_dict, already_reco_sids)
        if alt_combo_sids and combo_names:
            alt_combo_sids_list.append(alt_combo_sids)
            combo_name = self.alt_naming('nation+genre', **{'nation': combo_names[0], 'genre': combo_names[1]})
            combo_name_list.append(combo_name)

        # 2. genre - type
        alt_combo_sids, combo_names = self.make_alt_combo_A_B(user_profiling.genre_bucket, user_profiling.type_bucket, ranked_sid_index_dict,
                                    already_reco_sids)
        if alt_combo_sids and combo_names:
            alt_combo_sids_list.append(alt_combo_sids)
            combo_name = self.alt_naming('type+genre', **{'type': combo_names[0], 'genre': combo_names[1]})
            combo_name_list.append(combo_name)

        # 3. type
        alt_combo_sids, combo_name = self.make_alt_combo_A(user_profiling.type_bucket, ranked_sid_index_dict,
                                                              already_reco_sids)
        if alt_combo_sids and combo_name:
            alt_combo_sids_list.append(alt_combo_sids)
            combo_name = self.alt_naming("type", **{'type': combo_name})
            combo_name_list.append(combo_name)

        # 4. person
        for person_name_id in user_profiling.person_bucket:
            person_name_id = str(person_name_id[0])  # get '301150' from ('301150', 4.91)
            person = self.person_meta_dict.get(person_name_id, None)
            if person:

                combo_sid_pool = self.black_list_filtering(person.sids)
                combo_sid_pool = self.rm_series(combo_sid_pool)
                combo_sid_pool = set(combo_sid_pool) - already_reco_sids

                if len(combo_sid_pool) < self.min_nb_reco:
                    continue

                alt_sids = self.rank_sid_pool(list(combo_sid_pool), ranked_sid_index_dict)
                if len(alt_sids) < self.min_nb_reco:
                    continue

                already_reco_sids.update(alt_sids[:5])
                alt_combo_sids_list.append(alt_sids)
                combo_name_list.append(self.alt_naming("person", **{'person': person.person_name}))
                break

        # 5. nation
        alt_combo_sids, combo_name = self.make_alt_combo_A(user_profiling.nation_bucket, ranked_sid_index_dict,
                                                           already_reco_sids)
        if alt_combo_sids and combo_name:
            alt_combo_sids_list.append(alt_combo_sids)
            combo_name = self.alt_naming("nation", **{'nation': combo_name})
            combo_name_list.append(combo_name)

        # 6. genre
        alt_combo_sids, combo_name = self.make_alt_combo_A(user_profiling.genre_bucket, ranked_sid_index_dict,
                                                           already_reco_sids)
        if alt_combo_sids and combo_name:
            alt_combo_sids_list.append(alt_combo_sids)
            combo_name = self.alt_naming("genre", **{'genre': combo_name})
            combo_name_list.append(combo_name)

        return alt_combo_sids_list, combo_name_list
        """
        if user_profiling.nation_bucket and user_profiling.genre_bucket:
            for i in range(len(user_profiling.nation_bucket)):
                nation_name = user_profiling.nation_bucket[i]
                for j in range(len(user_profiling.genre_bucket)):
                    genre_name = user_profiling.genre_bucket[j]

                    # get a pool of SID candidates
                    combo_sid_pool = self.query_sid_pool([nation_name, genre_name], already_reco_sids)

                    if not combo_sid_pool:  # discard combo w/ < self.min_nb_reco
                        continue
                    else:
                        # rank sid and
                        alt_sids = self.rank_sid_pool(combo_sid_pool, ranked_sid_index_dict)
                        already_reco_sids.update(alt_sids[:5])
                        user_profiling.nation_bucket.pop(i)
                        user_profiling.genre_bucket.pop(j)
                        combo_name.append(self.alt_naming('nation+genre', **{'nation': nation_name, 'genre': genre_name}))
                        break
                else:
                    continue
                break


        # TODO: special case for HOUGA, YOUGA, DRAMA, ADRAMA, FDRAMA
        # 2. genre - type
        if user_profiling.genre_bucket and user_profiling.type_bucket:
            genre_name = user_profiling.genre_bucket.pop(0)[0]
            type_name =  user_profiling.type_bucket.pop(0)[0]
            combo_name.append(
                self.alt_naming('type+genre', **{'type': user_profiling.type_name_converting[type_name], 'genre': genre_name}))
            combo_query.append([genre_name, type_name])
            # combo_sids.append(sakuhin_query_table.do_query([genre_name, type_name]))

        # 3. type
        if user_profiling.type_bucket:
            type_name = user_profiling.type_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("type", **{'type': user_profiling.type_name_converting[type_name]}))
            combo_query.append([type_name])
            # combo_sids.append(sakuhin_query_table.do_query([type_name]))

        # 4. person
        if user_profiling.person_bucket:
            while user_profiling.person_bucket:
                person_name_id = user_profiling.person_bucket.pop(0)[0]
                person = self.person_meta_dict.get(person_name_id, None)
                if person:
                    combo_name.append(self.alt_naming("person", **{'person': person.person_name}))

                sids = self.black_list_filtering(person.sids)
                sids = self.rm_series(sids)

                if len(sids) < self.min_nb_reco:
                    continue
                else:
                    combo_query.append(sids)
                    print(f'person ALT = {sids}')
                    break
            # TODO: remove already_reco sids

        # 5. nation
        if user_profiling.nation_bucket:
            nation_name = user_profiling.nation_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("nation", **{'nation': nation_name}))
            combo_query.append([nation_name])

        # 6. genre
        if user_profiling.genre_bucket:
            genre_name = user_profiling.genre_bucket.pop(0)[0]
            combo_name.append(self.alt_naming("genre", **{'genre': genre_name}))
            combo_query.append([genre_name])

        # return combo_sids, combo_name
        for query, name in zip(combo_query, combo_name):
            if len(query) > 3:  # special case person.sids: don't need query
                combo_sid_pool = query
            else:
                combo_sid_pool = self.sakuhin_query_table.do_query(query)
            yield combo_sid_pool, name
        """

    def ippan_sakuhin(self,):
        tag_index_list = list(self.tag_index_dict.keys())
        logging.info(f"we got {len(tag_index_list)} Tags")

        logging.info("[Phase] - build tag index vector for all SIDs")
        # read sid cast first

        if not self.sid_cast_dict:
            for line in efficient_reading(self.path_params["sakuhin_cast_path"]):
                arr = line.rstrip().replace('"', '').split(",")
                sids = arr[2].split("|")
                self.sid_cast_dict[arr[1]] = sids

        # build sid_vector for all SIDs
        for line in efficient_reading(self.path_params["sakuhin_meta_path"], True):
            sid = line.split(",")[0]
            v = np.zeros(len(self.tag_index_dict))
            sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
            sid_cast = self.sid_cast_dict.get(sid, [])

            # label tag indice w/ 1
            for x in [sid_type] + list(sid_genres) + list(sid_nations) + sid_cast:
                tag_index = self.tag_index_dict.get(self.txt_processing(x), None)
                if tag_index:
                    v[tag_index] = 1
            self.sid_vector[sid] = v

        self.read_already_reco_sids(self.path_params["already_reco_path"])

        model = self.load_model(self.path_params["bpr_model_path"])

        # for bucket categorization
        if not self.type_set or not self.genre_set or not self.nation_set:
            for line in efficient_reading(self.path_params["sakuhin_meta_path"], True):
                sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
                self.type_set.add(sid_type)
                self.genre_set.update(sid_genres)
                self.nation_set.update(sid_nations)

        logging.info("[Phase] - making Tag ALTs, updating levelDB user_profiling_tags & levelDB JFET000006")
        nb_reco_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for line in efficient_reading(self.path_params["user_sid_history_path"], True):
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
                ranked_sid_list = None
                for userid, sid_list, score_list in rank(model, target_users=[userid],
                                                           filter_already_liked_items=True, top_n=-1, batch_size=10000):
                    ranked_sid_list = sid_list

                if not ranked_sid_list:
                    logging.debug(f"user:{userid} has no data in the model")
                    continue

                ranked_sid_index_dict = {sid:index for index, sid in enumerate(ranked_sid_list)}

                # prevent duplicates
                # read toppick[:5], trending[:5], popular[:5] as default, then update every Tag ALT
                # already_reco_sids = self.already_reco_dict.get(userid, set())
                # output_list = [ alt_name1:SIDa|SIDb|SIDc ]
                output_list = []

                combo_sid_list, combo_name_list = self.alt_combo_maker(user_profiling, ranked_sid_index_dict)

                for combo_sid, combo_name in zip(combo_sid_list, combo_name_list):
                    output_list.append(f"{combo_name}={'|'.join(combo_sid[:self.max_nb_reco])}")

                    if len(output_list) == 4:    # TODO
                        break
                if output_list:
                    self.JFET000006.put(userid.encode('utf-8'), '+'.join(output_list).encode('utf-8'))

                    # TODO: dev
                    #for i in range(len(output_list)):
                    #    w.write(f"{userid},{self.alt_info['feature_public_code'].values[0]}_{i + 1},{output_list[i]}\n")

                nb_reco_users += 1
                if nb_reco_users % 1000 == 1:
                    logging.info(f"{nb_reco_users} users got processed")
                    # break  # TODO:dev

        logging.info("{} users got reco / total nb of target user: {}, coverage rate={:.3f}".format(nb_reco_users, len(self.target_users),
                                                                                             nb_reco_users / len(self.target_users)))
    """
    def ippan_sakuhin(self,):
        tag_index_list = list(self.tag_index_dict.keys())
        logging.info(f"we got {len(tag_index_list)} Tags")

        logging.info("[Phase] - build tag index vector for all SIDs")
        # read sid cast first

        if not self.sid_cast_dict:
            for line in efficient_reading(self.path_params["sakuhin_cast_path"]):
                arr = line.rstrip().replace('"', '').split(",")
                sids = arr[2].split("|")
                self.sid_cast_dict[arr[1]] = sids

        # build sid_vector for all SIDs
        for line in efficient_reading(self.path_params["sakuhin_meta_path"], True):
            sid = line.split(",")[0]
            v = np.zeros(len(self.tag_index_dict))
            sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
            sid_cast = self.sid_cast_dict.get(sid, [])

            # label tag indice w/ 1
            for x in [sid_type] + list(sid_genres) + list(sid_nations) + sid_cast:
                tag_index = self.tag_index_dict.get(self.txt_processing(x), None)
                if tag_index:
                    v[tag_index] = 1
            self.sid_vector[sid] = v

        self.read_already_reco_sids(self.path_params["already_reco_path"])

        model = self.load_model(self.path_params["bpr_model_path"])

        # for bucket categorization
        if not self.type_set or not self.genre_set or not self.nation_set:
            for line in efficient_reading(self.path_params["sakuhin_meta_path"], True):
                sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
                self.type_set.add(sid_type)
                self.genre_set.update(sid_genres)
                self.nation_set.update(sid_nations)

        logging.info("[Phase] - making Tag ALTs, updating levelDB user_profiling_tags & levelDB JFET000006")
        nb_reco_users = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for line in efficient_reading(self.path_params["user_sid_history_path"], True):
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
                ranked_sid_list = None
                for userid, sid_list, score_list in rank(model, target_users=[userid],
                                                           filter_already_liked_items=True, top_n=-1, batch_size=10000):
                    ranked_sid_list = sid_list

                if not ranked_sid_list:
                    logging.debug(f"user:{userid} has no data in the model")
                    continue

                ranked_sid_index_dict = {sid:index for index, sid in enumerate(ranked_sid_list)}

                # prevent duplicates
                # read toppick[:5], trending[:5], popular[:5] as default, then update every Tag ALT
                already_reco_sids = self.already_reco_dict.get(userid, set())
                # output_list = [ alt_name1:SIDa|SIDb|SIDc ]
                output_list = []

                for combo_sid, combo_name in user_profiling.alt_combo_maker(self.person_meta_dict, self.sakuhin_query_table):
                    # sids = self.black_list_filtering(combo_sid)
                    # sids = self.rm_series(sids)
                    sids = combo_sid

                    #alt_sids = [sids[index] for index in np.argsort([ranked_sid_index_dict.get(sid, 99999) for sid in sids if sid not in already_reco_sids])][:20]

                    if len(sids) > 500:
                        n = 20
                        sid_ranking = [ranked_sid_index_dict[sid] for sid in sids if
                                       sid in ranked_sid_index_dict and sid not in already_reco_sids]
                        sid_ranking = np.array(sid_ranking)
                        top_ranking_indice = np.argpartition(sid_ranking, n)[:n]
                        alt_sids = [sids[i] for i in top_ranking_indice[np.argsort((sid_ranking)[top_ranking_indice])]]
                    else:
                        alt_sids = [sids[index] for index in np.argsort(
                            [ranked_sid_index_dict[sid] for sid in sids if sid in ranked_sid_index_dict and sid not in already_reco_sids])][:20]

                    if len(alt_sids) < self.min_nb_reco:
                        logging.debug(f"{combo_name} got too less SIDs")
                        continue

                    already_reco_sids.update(alt_sids[:5])

                    output_list.append(f"{combo_name}={'|'.join(alt_sids[:self.max_nb_reco])}")

                    if len(output_list) == 4:    # TODO
                        break
                if output_list:
                    self.JFET000006.put(userid.encode('utf-8'), '+'.join(output_list).encode('utf-8'))

                    # TODO: dev
                    #for i in range(len(output_list)):
                    #    w.write(f"{userid},{self.alt_info['feature_public_code'].values[0]}_{i + 1},{output_list[i]}\n")

                nb_reco_users += 1
                if nb_reco_users % 1000 == 1:
                    logging.info(f"{nb_reco_users} users got processed")
                    # break  # TODO:dev

        logging.info("{} users got reco / total nb of target user: {}, coverage rate={:.3f}".format(nb_reco_users, len(self.target_users),
                                                                                             nb_reco_users / len(self.target_users)))
    """
    def txt_processing(self, txt):
        return txt.replace("|", "&")  # avoid containing separator '|'

    def tags_preprocessing(self, line):
        # sakuhin_public_code,display_name,main_genre_code,menu_names,parent_menu_names,nations
        arr = line.rstrip().split(",")

        # preprocess for main_genre_code
        sid_type = type_naming_converter(arr[-4])  # .lower()

        # preprocess for menu_names & parent_menu_names
        sid_genres = set()
        for menu_name in arr[3].split("|"):  # + arr[4].split("|"):
            sid_genres.add(menu_name)

        # preprocess for nations
        sid_nations = []
        if arr[5] != '':
            sid_nations = arr[5].split("/")

        return sid_type, sid_genres, set(sid_nations)

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
                            f"{userid},{self.alt_info['feature_public_code'].values[0]}{i + 1},{self.create_date},{output_arr[1]},"
                            f"{combo_name},{self.alt_info['domain'].values[0]},1,"
                            f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

