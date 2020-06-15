import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import operator
import pickle
from mappings import types, tpye_mapping, genres_mapping, tags

logger = logging.getLogger(__name__)


#UNEXT_ANALYTICS_DW_PROD = "postgres://reco_etl:recoreco@10.232.201.241:5432/unext_analytics_dw"
#engine = create_engine(UNEXT_ANALYTICS_DW_PROD)


sql_metatable = """
with main_code as (
  select
    distinct menu_public_code
  from dim_sakuhin_menu
  where parent_menu_public_code is null
), tags as (
  select
    distinct
    parent_menu_public_code,
    parent_menu_name,
    dsm.menu_public_code,
    menu_name
  from dim_sakuhin_menu dsm
  inner join main_code mc
  on dsm.parent_menu_public_code = mc.menu_public_code
  where menu_name not like '%%ランキング%%'
  and menu_name not like '%%作品%%'
  and menu_name not like '%%歳%%'
  and menu_name not like '%%行'
  and parent_menu_public_code in (
  'MNU0000001',
  'MNU0000018',
  'MNU0000035',
  'MNU0000050',
  'MNU0000063',
  'MNU0000076',
  'MNU0000090',
  'MNU0000102',
  'MNU0000117',
  'MNU0000124'
))
select
  dm.sakuhin_public_code,
  dm.display_name,
  dm.main_genre_code,
  -- tags.menu_public_code,
  array_to_string(array_agg(tags.menu_name),'|') as menu_names,
  -- tags.parent_menu_public_code,
  array_to_string(array_agg(tags.parent_menu_name), '|') as parent_menu_names,
  current_sakuhin.display_production_country as nations
from dim_sakuhin_menu
inner join tags using(menu_public_code)
right join (
  select
    distinct sakuhin_public_code,
    display_production_country
  from dim_product
--  where sale_end_datetime >= now()
--  and sale_start_datetime < now()
  ) current_sakuhin using(sakuhin_public_code)
inner join dim_sakuhin dm using(sakuhin_public_code)
group by dm.sakuhin_public_code, dm.display_name, dm.main_genre_code, nations
--where sakuhin_public_code = 'SID0002167'
order by sakuhin_public_code asc
"""


sql_lookuptable = """
with main_code as (
  select
    distinct menu_public_code
  from dim_sakuhin_menu
  where parent_menu_public_code is null
), tags as (
  select
    distinct
    parent_menu_public_code,
    parent_menu_name,
    dsm.menu_public_code,
    menu_name
  from dim_sakuhin_menu dsm
  inner join main_code mc
  on dsm.parent_menu_public_code = mc.menu_public_code
  where menu_name not like '%%ランキング%%'
  and menu_name not like '%%作品%%'
  and menu_name not like '%%歳%%'
  and menu_name not like '%%行'
  and parent_menu_public_code in (
  'MNU0000001',
  'MNU0000018',
  'MNU0000035',
  'MNU0000050',
  'MNU0000063',
  'MNU0000076',
  'MNU0000090',
  'MNU0000102',
  'MNU0000117',
  'MNU0000124'
))
select
  dm.sakuhin_public_code,
  dm.display_name,
  dm.main_genre_code,
  -- tags.menu_public_code,
  tags.menu_name,
  -- array_to_string(array_agg(tags.menu_name),'|') as menu_names,
  -- tags.parent_menu_public_code,
  tags.parent_menu_name,
  -- array_to_string(array_agg(tags.parent_menu_name), '|') as parent_menu_names,
  current_sakuhin.display_production_country as nations
from dim_sakuhin_menu
inner join tags using(menu_public_code)
right join (
  select
    distinct sakuhin_public_code,
    display_production_country
  from dim_product
--  where sale_end_datetime >= now()
--  and sale_start_datetime < now()
  ) current_sakuhin using(sakuhin_public_code)
inner join dim_sakuhin dm using(sakuhin_public_code)
-- group by dm.sakuhin_public_code, dm.display_name, dm.main_genre_code, nations
--where sakuhin_public_code = 'SID0002167'
order by sakuhin_public_code asc
"""


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



def video_all_genre_rows(nb_alt, max_nb_reco, min_nb_reco,
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

    # 4.
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
    if ALT_domain == "video_all":
        video_all_genre_rows(nb_alt, max_nb_reco, min_nb_reco,
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














