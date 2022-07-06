import os, sys
import logging
import pandas as pd
import numpy as np
from collections import Counter, defaultdict
from autoalts.autoalt_maker import AutoAltMaker
import plyvel

PROJECT_PATH = os.path.abspath("%s/.." % os.path.dirname(__file__))
sys.path.append(PROJECT_PATH)
from autoalts.utils import efficient_reading
from ranker import Ranker

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
        print("metric_name: ", self.type_bucket)
        print("person: ", self.person_bucket)


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
                         kwargs["record_path"], kwargs["max_nb_reco"], kwargs["min_nb_reco"])

        self.path_params = {}

        self.batch_size = int(kwargs["batch_size"])

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
        self.path_params = kwargs

        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.sakuhin_query_table = SIDQueryTable(self.path_params["lookup_table_path"])

            if not self.tag_index_dict:
                self.make_tag_index_dict()

            # update levelDB JFET000006 & user_profiling_tags
            self.ippan_sakuhin()
            self.reco_record.close()

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
        :param combo: e.g. nation:日本-genre:action / genre:action-metric_name:anime  / metric_name:anime
        :return:
        """
        if combo == "nation+metric_name":
            return f"{kwargs['metric_name']}/{kwargs['nation']}のオススメ"
        elif combo == "nation+genre":
            return f"{kwargs['nation']}/{kwargs['genre']}"
        elif combo == "metric_name+genre" or combo == "genre+metric_name":
            return f"{kwargs['metric_name']}/{kwargs['genre']}"
        elif combo == "nation":
            return f"製作国:{kwargs['nation']}のピックアップ"
        elif combo == "metric_name":
            return f"{kwargs['metric_name']}のオススメ"
        elif combo == "genre":
            return f"{kwargs['genre']}のオススメ"
        elif combo == "person":
            return f"「{kwargs['person']}」のピックアップ作品"
        else:
            raise Exception(f"WRONG COMBO {combo}")

    def query_sid_pool(self, query, already_reco_sids=None):
        """

        :param query: e.g. ["アニメ"]
        :param already_reco_sids:
        :return:
        """
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

    def alt_combos(self, user_profiling):
        # Rule-base version
        combo_name_list = []

        # 1. nation - genre
        if user_profiling.nation_bucket and user_profiling.genre_bucket:
            combo_name_list.append(f'nation+{user_profiling.nation_bucket.pop(0)[0]}+genre+{user_profiling.genre_bucket.pop(0)[0]}')

        # 2. genre - metric_name
        if user_profiling.genre_bucket and user_profiling.type_bucket:
            combo_name_list.append(f'genre+{user_profiling.genre_bucket.pop(0)[0]}+metric_name+{user_profiling.type_bucket.pop(0)[0]}')

        # 3. metric_name
        if user_profiling.type_bucket:
            combo_name_list.append(f'metric_name+{user_profiling.type_bucket.pop(0)[0]}')

        # 4. person
        if user_profiling.person_bucket:
            person_name_id = str(user_profiling.person_bucket.pop(0)[0])  # get '301150' from ('301150', 4.91)
            combo_name_list.append(f'person+{person_name_id}')  # person is an obj

        # 5. nation
        if user_profiling.nation_bucket:
            combo_name_list.append(f'nation+{user_profiling.nation_bucket.pop(0)[0]}')

        # 6. genre
        if user_profiling.genre_bucket:
            combo_name_list.append(f'genre+{user_profiling.genre_bucket.pop(0)[0]}')

        return combo_name_list

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

        # for bucket categorization
        if not self.type_set or not self.genre_set or not self.nation_set:
            for line in efficient_reading(self.path_params["sakuhin_meta_path"], True):
                sid_type, sid_genres, sid_nations = self.tags_preprocessing(line)
                self.type_set.add(sid_type)
                self.genre_set.update(sid_genres)
                self.nation_set.update(sid_nations)

        logging.info("[Phase] - making Tag ALTs, updating levelDB user_profiling_tags & levelDB JFET000006")

        # RECALL part
        combo_user_dict = self.recall(tag_index_list)
        logging.info(f'total nb of ALTs: {len(combo_user_dict)} ALTs')  # 5694 ALTs

        # RANK part
        self.rank(combo_user_dict, model_path=self.path_params["model_path"])

        # RERANK & output part
        self.rerank_and_output_from_leveldb()

    def recall(self, tag_index_list):
        nb_lines = 0
        combo_user_dict = defaultdict(list)

        # for each line (daily watching history of a user)
        for line in efficient_reading(self.path_params["user_sid_history_path"], True):
            userid = line.split(",")[0]

            if self.target_users and userid not in self.target_users:
                continue

            # clear the user reco in leveldb
            self.JFET000006.put(userid.encode('utf-8'), ''.encode('utf-8'))

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
            # build user_profiling by adding top Tags
            for index_tag in (np.argsort(-user_profiling_vector))[:20]:
                if user_profiling_vector[index_tag] < 0.01:
                    break
                user_profiling.add(tag_index_list[index_tag], user_profiling_vector[index_tag],
                                   self.type_set, self.genre_set, self.nation_set)

            # -> {nation+genre: (user_id, ALT_name)}
            for i, alt_combo in enumerate(self.alt_combos(user_profiling)):
                combo_user_dict[alt_combo].append(userid)

            nb_lines += 1
            if nb_lines % 1000 == 1:
                logging.info(f"{nb_lines} lines got processed")
            # if nb_lines > 100:
            #    break  # TODO: dev break
        return combo_user_dict

    def rank(self, combo_user_dict, model_path):
        ranker = Ranker(model_path=model_path)

        for alt_combo, user_id_list in combo_user_dict.items():
            arr = alt_combo.split("+")
            if len(arr) == 4:
                # query_sid_pool: query, remove blacklist, remove same series, return None if < self.min_nb_reco
                pool_SIDs = self.query_sid_pool([arr[1], arr[3]])
                combo_name = self.alt_naming(f'{arr[0]}+{arr[2]}', **{arr[0]: arr[1], arr[2]: arr[3]})
            elif len(arr) == 2:
                if arr[0] == 'person':
                    person = self.person_meta_dict.get(arr[1], None)
                    if person:
                        combo_name = self.alt_naming(arr[0], **{arr[0]: person.person_name})
                        pool_SIDs = person.sids
                    else:
                        continue
                else:
                    combo_name = self.alt_naming(arr[0], **{arr[0]: arr[1]})
                    pool_SIDs = self.query_sid_pool([arr[1]])
            else:
                raise Exception(f"alt_combo wrong in {alt_combo}")

            pool_SIDs = self.black_list_filtering(pool_SIDs)
            if not pool_SIDs:  # discard combo w/ < self.min_nb_reco
                logging.debug(f"discard {alt_combo} due to no combo_sid_pool")
                continue
            else:
                logging.debug(f"rank {combo_name} with {len(pool_SIDs)} pool_SIDs for {len(user_id_list)} users")

                for userid, sid_list in ranker.rank(target_users=user_id_list, target_items=pool_SIDs,
                                                    filter_already_liked_items=True, batch_size=self.batch_size):

                    if not userid or not sid_list:
                        logging.info("not userid or not sid_list")
                        break

                    user_reco = self.JFET000006.get(userid.encode('utf-8'), b'').decode("utf-8")

                    if not user_reco:
                        user_reco = f"{combo_name}={'|'.join(sid_list[:self.max_nb_reco])}"
                    else:
                        user_reco = user_reco + "+" + f"{combo_name}={'|'.join(sid_list[:self.max_nb_reco])}"

                    self.JFET000006.put(userid.encode('utf-8'), user_reco.encode('utf-8'))

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

    def rerank_and_output_from_leveldb(self):
        logging.info("output to JFET000006.csv from levelDB JFET000006")

        user_cnt = 0

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for key, value in self.JFET000006:
                userid = key.decode('utf-8')

                if self.target_users and userid not in self.target_users:
                    continue

                user_cnt += 1
                output_list = value.decode("utf-8")
                if output_list:
                    for i, output_str in enumerate(output_list.split("+")):

                        # TODO: control the nb of tag alts here
                        if i > 4:  # JFET0000061 ~ JFET0000063
                            break

                        output_arr = output_str.split("=")
                        combo_name = output_arr[0]

                        # blacklist removal, same series removal are done by query_sid_pool
                        # remove already reco
                        # reco_sids = [sid for sid in output_arr[1].split("|") if sid not in already_reco_sids]
                        reco = output_arr[1].split("|")

                        # remove blacklist & sids got reco already
                        reco = self.remove_black_duplicates(userid, reco)

                        if len(reco) < self.min_nb_reco:
                            continue
                        else:
                            # update reco_record
                            self.reco_record.update_record(userid, sids=reco, all=False)

                        # w.write(self.output_reco(userid, reco_sids))
                        w.write(
                            f"{userid},{self.alt_info['feature_public_code'].values[0]}{i + 1},{self.create_date},"
                            f"{'|'.join(reco[:self.max_nb_reco])},"
                            f"{combo_name},{self.alt_info['domain'].values[0]},1,"
                            f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

        logging.info(f"{user_cnt} users got Tag ALTs")

