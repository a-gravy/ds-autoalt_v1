import logging
import pandas as pd
import numpy as np
import operator
from collections import Counter
import itertools
from autoalts.autoalt_maker import AutoAltMaker
import autoalts.mappings as mapping
from bpr.implicit_recommender import rerank
from utils import efficient_reading

logger = logging.getLogger(__name__)


class TagAlt(AutoAltMaker):
    def __init__(self, alt_info, create_date, target_users_path, blacklist_path, series_path=None, max_nb_reco=20,
                 min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)
        self.target_users = None
        if target_users_path:
            self.target_users = self.read_target_users(target_users_path)

        self.tag_index_dict = {}  # e.g. {'日本': 0}
        self.sid_vector = {}  # {sid: [0, 1, 1, 0, ...]}
        self.types = [s.lower() for s in mapping.types]
        self.lookup = None

    def make_alt(self, bpr_model_path, sakuhin_meta_path='data/sakuhin_meta.csv',
                 lookup_table_path="data/sakuhin_lookup_table.csv",
                 user_sid_history_path='data/user_sid_history.csv'):
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.build_lookup(lookup_table_path)
            self.ippan_sakuhin(bpr_model_path, sakuhin_meta_path, user_sid_history_path)
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "adult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def ippan_sakuhin(self, bpr_model_path, sakuhin_meta_path, user_sid_history_path):
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
        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", "w") as w:
            w.write(self.config['header']['feature_table'])
            for line in efficient_reading(user_sid_history_path, True):
                userid = line.split(",")[0]
                user_profiling_vector = np.zeros(len(self.tag_index_dict))
                # build user vector based on history (from old -> new)
                for sid in line.rstrip().split(",")[1].split("|")[::-1]:
                    user_profiling_vector = user_profiling_vector * 0.9 + self.sid_vector[sid]

                # build Tag combo based on user profile
                user_profiling_tags_dict = {}  # {'nation': ['日本'], 'type': ['anime', 'kids', 'drama'], ...
                for index_tag in (np.argsort(-user_profiling_vector))[:20]:
                    if user_profiling_vector[index_tag] < 0.01:
                        break
                    types = [s.lower() for s in mapping.types]
                    if tag_index_list[index_tag].lower() in types:
                        tag_type = 'type'
                    elif tag_index_list[index_tag] in mapping.genres:
                        tag_type = 'genre'
                    elif tag_index_list[index_tag] in mapping.tags:
                        tag_type = 'tags'
                    else:
                        tag_type = 'nation'
                    user_profiling_tags_dict[tag_type] = user_profiling_tags_dict.setdefault(tag_type, []) + [
                        tag_index_list[index_tag]]
                    # print(f"{tag_type}-{tag_index_list[index_tag]} w/ {user_profiling_vector[index_tag]}")

                # make ALT combo(e.g. genre:action-type:anime) based on user_profiling_tags_dict
                alt_combos, nation_genre, genre_type, nation_type = [], [], [], []
                nation_combo = []

                if 'nation' in user_profiling_tags_dict and 'genre' in user_profiling_tags_dict:
                    for nation in user_profiling_tags_dict['nation']:
                        for combo in user_profiling_tags_dict['genre']:
                            nation_combo.append(f'nation:{nation}-genre:{combo}')

                if 'nation' in user_profiling_tags_dict and 'type' in user_profiling_tags_dict:
                    for nation in user_profiling_tags_dict['nation']:
                        for combo in user_profiling_tags_dict['type']:
                            nation_combo.append(f'nation:{nation}-type:{combo}')

                if 'genre' in user_profiling_tags_dict and 'type' in user_profiling_tags_dict:
                    for genre in user_profiling_tags_dict['genre']:
                        for combo in user_profiling_tags_dict['type']:
                            genre_type.append(f'genre:{genre}-type:{combo}')

                for combo_1, combo_2 in itertools.zip_longest(genre_type, nation_combo, fillvalue=None):
                    if combo_1:
                        alt_combos.append(combo_1)
                    if combo_2:
                        alt_combos.append(combo_2)

                bpr_sid_list = None
                for userid, sid_list, score_list in rerank(model, target_users=[userid],
                                                           filter_already_liked_items=True, top_n=-1, batch_size=10000):
                    bpr_sid_list = sid_list

                if not bpr_sid_list:
                    logging.info(f"user:{userid} has no data in model")
                    continue
                bpr_sid_set = set(bpr_sid_list)

                already_reco_sids = set()

                for combo in alt_combos[:3]:  # TODO 3 only
                    part_1 = combo.split('-')[0].split(':')
                    part_2 = combo.split('-')[1].split(':')
                    condition = (self.lookup[part_1[0]] == part_1[1]) & (self.lookup[part_2[0]] == part_2[1])
                    sids = self.do_query(condition)
                    if not sids:
                        combo = combo.split('-')[1]
                        condition = self.lookup[part_2[0]] == part_2[1]
                        sids = self.do_query(condition)
                    alt_sids = [sids[index] for index in np.argsort(
                        [bpr_sid_list.index(sid) for sid in sids if sid in bpr_sid_set and sid not in already_reco_sids])][:20]
                    already_reco_sids.update(alt_sids[:10])

                    # print(f'{combo}  {alt_sids}')
                    w.write(
                        f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(alt_sids[:self.max_nb_reco])},"
                        f"{combo},{self.alt_info['domain'].values[0]},1,"
                        f"{self.config['feature_public_start_datetime']},{self.config['feature_public_end_datetime']}\n")

    def tags_preprocessing(self, line):
        # sakuhin_public_code,display_name,main_genre_code,menu_names,parent_menu_names,nations
        arr = line.rstrip().split(",")

        # preprocess for main_genre_code
        sid_type = mapping.tpye_mapping.get(arr[2], arr[2]).lower()

        # preprocess for menu_names & parent_menu_names
        sid_tags = set([sid_type])
        for menu_name in arr[3].split("|") + arr[4].split("|"):
            genre = mapping.genres_mapping.get(menu_name, None)
            if genre:
                sid_tags.add(genre)
            elif menu_name in mapping.tags:
                sid_tags.add(menu_name)

        # preprocess for nations
        if arr[5] != '':
            for nation in arr[5].split("/"):
                sid_tags.add(nation)

        return sid_tags

    def build_lookup(self, lookup_table_path):
        self.lookup = pd.read_csv(lookup_table_path)

        def type_mapping(typename):
            return mapping.tpye_mapping.get(typename, typename)

        def genre_mapping(menu_name):
            genre = mapping.genres_mapping.get(menu_name, None)
            if genre:
                return genre
            else:
                return None

        def tag_mapping(menu_name):
            if menu_name in mapping.tags:
                return menu_name
            else:
                return None

        self.lookup['genre'] = list(map(genre_mapping, self.lookup['menu_name'].str.lower()))
        self.lookup['tag'] = list(map(tag_mapping, self.lookup['menu_name'].str.lower()))
        self.lookup['type'] = list(map(type_mapping, self.lookup['main_genre_code'].str.lower()))
        logging.info("ALT lookup table built")

    def do_query(self, condition):
        x = list(self.lookup[condition]['sakuhin_public_code'].unique())
        return x





class VideoFeatures:  # sakuhin_dict{sakuhin_public_code: VideoFeatures}
    def __init__(self, arrs):
        self.type = ""
        self.nations = []
        self.genre = ''
        self.tags = []

        # arrs: sakuhin_public_code, display_name, main_genre_code, menu_names, parent_menu_names, nations
        self.type = self.type_mapping(arrs[2])

        if arrs[3] != '':
            for menu_name in arrs[3].split("|"):
                key, value = self.menu_names_mapping(menu_name)
                if key and key == "genre":
                    self.genre = value
                elif key and key == "tag":
                    self.tags.append(value)

        if arrs[5] != '':
            for nation in arrs[5].split("|"):
                self.nations.append(self.nation_mapping(nation))

        """
        self.type = self.type_mapping(row['main_genre_code'])

        for menu_name in row['menu_names'].split("|"):
            key, value = self.menu_names_mapping(menu_name)
            if key and key == "genre":
                self.genre = value
            elif key and key == "tag":
                self.tags.append(value)

        if row['nations'] and not isinstance(row['nations'], type(None)):
            for nation in row['nations'].split("|"):
                self.nations.append(self.nation_mapping(nation))
        """

    def add(self, row):
        pass

    def type_mapping(self, typename):
        return tpye_mapping.get(typename, typename)

    def nation_mapping(self, nation):  # TODO: nation mapping
        return nation

    def menu_names_mapping(self, menu_name):
        genre = genres_mapping.get(menu_name, None)
        if genre:
            return 'genre', genre
        elif menu_name in tags:
            return 'tag', menu_name
        else:
            return None, None

    def info(self):
        logging.info("type = ", self.type)
        logging.info("nations = ", self.nations)
        logging.info("genre = ", self.genre)
        logging.info("tags = ", self.tags)


class UserStatistics_v2:
    def __init__(self, uid, sakuhin_dict):
        self.uid = uid
        self.sakuhin_dict = sakuhin_dict
        self.types = {}  # movie:xx sec
        self.nations = {}  # japan:xx sec
        self.genre = {}  # action:xx sec
        self.tags = {}  # sf:xx sec

    def add(self, sid, playback_time):  # add one record ex: 14736, 30
        # sakuhin_dict[sid] get VideoFeatures object
        # add VideoFeatures.type to self.types, and others so on
        sid = str(sid)

        sakuhin = self.sakuhin_dict.get("SID" + "0" * (7 - len(sid)) + sid, None)
        if not sakuhin:
            logging.info('can not find {}'.format("SID" + "0" * (7 - len(sid)) + sid))
            return

        if sakuhin.type:
            self.types[sakuhin.type] = self.types.setdefault(sakuhin.type, 0) + int(playback_time)
        if sakuhin.genre:
            self.genre[sakuhin.genre] = self.genre.setdefault(sakuhin.genre, 0) + int(playback_time)
        for nation in sakuhin.nations:
            self.nations[nation] = self.nations.setdefault(nation, 0) + int(playback_time)
        for tag in sakuhin.tags:
            self.tags[tag] = self.tags.setdefault(tag, 0) + int(playback_time)

    def info(self):
        print("types = {}".format(self.types))
        print("nations = {}".format(self.nations))
        print("genre = {}".format(self.genre))
        print("tags = {}".format(self.tags))

    def rank_info(self):  # rank all features, return list of (watch_length, region, America)
        # TODO: can be comprehension
        rank_dict = self.types.copy()
        rank_dict.update(self.nations)
        rank_dict.update(self.genre)
        rank_dict.update(self.tags)
        for k, v in sorted(rank_dict.items(), key=operator.itemgetter(1), reverse=True):
            print("[{}] {:.4f}".format(k, v))

    def do_normalization(self, d):  # normalize playback_time to %
        if not d or sum(d.values()) == 0:
            return None
        factor = 1.0 / sum(d.values())
        for k in d:
            d[k] = d[k] * factor
        return 1

    def normalization_info(self):
        if self.types:
            self.do_normalization(self.types)
        if self.nations:
            self.do_normalization(self.nations)
        if self.genre:
            self.do_normalization(self.genre)
        if self.tags:
            self.do_normalization(self.tags)

    def make_alt(self):
        alt_dict = {}  # "_-_":score

        # logic: type x nation (Anime x Japan)
        for type_k, type_v in self.types.items():
            for nations_k, nations_v in self.nations.items():
                alt_dict["{}-{}-{}-{}".format("type", type_k, "nations", nations_k)] = type_v * nations_v

        # logic: type x genre (Anime x romance)
        for type_k, type_v in self.types.items():
            for genre_k, genre_v in self.genre.items():
                alt_dict["{}-{}-{}-{}".format("type", type_k, "genre", genre_k)] = type_v * genre_v

        # logic: genre x nations_v (romance x Japan)
        for nations_k, nations_v in self.nations.items():
            for genre_k, genre_v in self.genre.items():
                alt_dict["{}-{}-{}-{}".format("nations", nations_k, "genre", genre_k)] = nations_v * genre_v

        # logic: tag  x type (特撮・ヒーロー x drama)
        for type_k, type_v in self.types.items():
            for tag_k, tag_v in self.tags.items():
                alt_dict["{}-{}-{}-{}".format("type", type_k, "tag", tag_k)] = type_v * tag_v

        # normalize playback time
        if not self.do_normalization(alt_dict):
            print("user:{} has no alt made".format(self.uid))
            return None
        else:
            top_n_alt = {}
            import operator
            # TODO: now top 5 alts are made
            for k, v in sorted(alt_dict.items(), key=operator.itemgetter(1), reverse=True):
                top_n_alt.setdefault(k, v)
                if len(top_n_alt) >= 5:
                    break

            return top_n_alt


class UserStatistics:
    def __init__(self, uid, sakuhin_dict):
        self.uid = uid
        self.sakuhin_dict = sakuhin_dict
        self.types = {}  # movie:xx sec
        self.nations = {}  # japan:xx sec
        self.genre = {}  # action:xx sec
        self.tags = {}  # sf:xx sec

    def add(self, sid, playback_time):
        # sakuhin_dict[sid] get VideoFeatures object
        # add VideoFeatures.type to self.types, and others so on

        sakuhin = self.sakuhin_dict.get(sid, None)
        if not sakuhin:
            logging.info('can not find {}'.format(sid))
            return

        playback_time = float(playback_time)
        if sakuhin.type:
            self.types[sakuhin.type] = self.types.setdefault(sakuhin.type, 0) + (playback_time)
        if sakuhin.genre:
            self.genre[sakuhin.genre] = self.genre.setdefault(sakuhin.genre, 0) + (playback_time)
        for nation in sakuhin.nations:
            self.nations[nation] = self.nations.setdefault(nation, 0) + (playback_time)
        for tag in sakuhin.tags:
            self.tags[tag] = self.tags.setdefault(tag, 0) + (playback_time)

    def info(self):
        logging.info("types = {}".format(self.types))
        logging.info("nations = {}".format(self.nations))
        logging.info("genre = {}".format(self.genre))
        logging.info("tags = {}".format(self.tags))

    def rank_info(self):  # rank all features, return list of (watch_length, region, America)
        # TODO: can be comprehension
        rank_dict = self.types.copy()
        rank_dict.update(self.nations)
        rank_dict.update(self.genre)
        rank_dict.update(self.tags)
        for k, v in sorted(rank_dict.items(), key=operator.itemgetter(1), reverse=True):
            print("[{}] {:.4f}".format(k, v))

    def do_normalization(self, d):  # normalize playback_time to %
        if not d or sum(d.values()) == 0:
            return None
        factor = 1.0 / sum(d.values())
        for k in d:
            d[k] = d[k] * factor
        return 1

    def normalization_info(self):
        if self.types:
            self.do_normalization(self.types)
        if self.nations:
            self.do_normalization(self.nations)
        if self.genre:
            self.do_normalization(self.genre)
        if self.tags:
            self.do_normalization(self.tags)

    def make_alt(self, top_n=3):
        alt_dict = {}  # "_-_":score

        # logic: type x nation (Anime x Japan)
        for type_k, type_v in self.types.items():
            for nations_k, nations_v in self.nations.items():
                alt_dict["{}-{}-{}-{}".format("type", type_k, "nations", nations_k)] = type_v * nations_v

        # logic: type x genre (Anime x romance)
        for type_k, type_v in self.types.items():
            for genre_k, genre_v in self.genre.items():
                alt_dict["{}-{}-{}-{}".format("type", type_k, "genre", genre_k)] = type_v * genre_v

        # logic: genre x nations_v (romance x Japan)
        for nations_k, nations_v in self.nations.items():
            for genre_k, genre_v in self.genre.items():
                alt_dict["{}-{}-{}-{}".format("nations", nations_k, "genre", genre_k)] = nations_v * genre_v

        # logic: tag  x type (特撮・ヒーロー x drama)
        for type_k, type_v in self.types.items():
            for tag_k, tag_v in self.tags.items():
                alt_dict["{}-{}-{}-{}".format("type", type_k, "tag", tag_k)] = type_v * tag_v

        # normalize playback time
        if not self.do_normalization(alt_dict):
            print("user:{} has no alt made".format(self.uid))
            return None
        else:
            top_n_alt = {}
            import operator
            for k, v in sorted(alt_dict.items(), key=operator.itemgetter(1), reverse=True):
                top_n_alt.setdefault(k, v)
                if len(top_n_alt) >= top_n:
                    break

            return top_n_alt


def video_genre_rows(nb_alt, max_nb_reco, min_nb_reco,
                         unext_sakuhin_meta_path="data/unext_sakuhin_meta.csv",
                         meta_lookup_path="data/unext_sakuhin_meta_lookup.csv",
                         user_sessions_path="data/user_sessions.csv",
                         output_path="data/genre_rows.csv"):
    """

    :param unext_sakuhin_meta_path:
    :param meta_lookup_path:
    :param user_sessions_path:
    :param output_path:
    :param nb_genre_row: nb of genre alt for every user
    :param nb_neco: nb of sakuhins in one genre alt
    :return:
    """
    # 1. organize meta information for each sakuhin
    # sakuhin_dict {sakuhin_public_code: VideoFeatures}
    sakuhin_dict = {}
    with open(unext_sakuhin_meta_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().split(",")
                sakuhin_dict[arr[0]] = VideoFeatures(arr)
            else:
                break

    # 2. make lookup table #
    lookup = pd.read_csv(meta_lookup_path)

    def type_mapping(typename):
        return tpye_mapping.get(typename, typename)

    def genre_mapping(menu_name):
        genre = genres_mapping.get(menu_name, None)
        if genre:
            return genre
        else:
            return None

    def tag_mapping(menu_name):
        if menu_name in tags:  # TODO: convert to english
            return menu_name
        else:
            return None

    lookup['genre'] = list(map(genre_mapping, lookup['menu_name']))
    lookup['tag'] = list(map(tag_mapping, lookup['menu_name']))
    lookup['type'] = list(map(type_mapping, lookup['main_genre_code']))

    logging.info("{} sakuhins metadata is ready".format(len(sakuhin_dict)))

    # 3. make statistics of every user's watch history #
    alt_dict = {}  # alt_dict = {alt: [user_id, score]}
    with open(user_sessions_path, "r") as r:
        r.readline()
        counter=0
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().replace('"', '').split(',')
                nb = len(arr) - 1
                sid_list = arr[1:1 + int(nb / 3)]
                watch_time_list = arr[1 + int(nb/3)*2:]
                assert len(sid_list) == len(watch_time_list), "fail@{} line, user:{},  {} - {}".format(
                    line, arr[0], len(sid_list), len(watch_time_list))

                stat = UserStatistics(arr[0], sakuhin_dict)

                # add every session to user's statistics
                for sid, watch_time in zip(sid_list,  watch_time_list):
                    stat.add(sid, watch_time)

                # get top N favorite genre rows
                user_alts = stat.make_alt(top_n=nb_alt)

                if user_alts:
                    for alt, score in user_alts.items():
                        alt_dict.setdefault(alt, [[], []])
                        alt_dict[alt][0].append(arr[0])
                        alt_dict[alt][1].append(score)

                if counter%1000==0:
                    logging.info("statistics of {} users done".format(counter))
                counter+=1
            else:
                break

    def do_query(condition):
        x = list(lookup[condition]['sakuhin_public_code'].unique())
        return x

    # 4. output genrerows for reranking
    nb_genre_row_written = 0
    with open(output_path, "w") as w:  # alt, sid list, uid list, score list
        for alt, lists in alt_dict.items():
            terms = alt.split("-")
            for x in lists[0]:
                if not isinstance(x, type("x")):
                    print("{} is not str".format(x))
            condition = (lookup[terms[0]] == terms[1]) & (lookup[terms[2]] == terms[3])
            query_sid_list = do_query(condition)
            if len(query_sid_list) < min_nb_reco:
                continue
            w.write("{},{},{},{}\n".format(alt, "|".join(query_sid_list),
                                           "|".join([str(x) for x in lists[0]]),
                                        "|".join([str(x) for x in lists[1]])))
            nb_genre_row_written += 1
    logging.info("nb_genre_row_written = {}".format(nb_genre_row_written))


def user_session_data_checker(user_sessions_path="data/user_sessions.csv"):
    with open(user_sessions_path, "r") as r:
        r.readline()
        counter = 0
        while True:
            line = r.readline()
            if line:
                arrs = line.rstrip().replace('"', '').split(',')
                sid_list, watch_time_list = [], []
                for v in arrs[1:]:
                    if "SID" in v:
                        sid_list.append(v)
                    else:
                        watch_time_list.append(v)
                assert len(sid_list) == len(watch_time_list), "fail@{} line, user:{},  {} - {}".format(
                    line, arrs[0], len(sid_list), len(watch_time_list))
                counter += 1
            else:
                break
    logging.info("check done, no problem")


def make_alt(ALT_code, ALT_domain, nb_alt, max_nb_reco, min_nb_reco,
             unext_sakuhin_meta_path, meta_lookup_path, user_sessions_path):
    if ALT_domain == "video":
        video_genre_rows(nb_alt, max_nb_reco, min_nb_reco,
                             unext_sakuhin_meta_path, meta_lookup_path,
                             user_sessions_path, output_path="data/genre_rows.csv")
    elif ALT_domain == "book_all":
        raise Exception("Not implemented yet")
    else:
        raise Exception("unknown ALT_domain")


def main():
    user_session_data_checker()


if __name__ == '__main__':
    main()














