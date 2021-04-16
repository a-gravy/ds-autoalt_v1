"""
Our definition of type, genres and tags
"""

def dict_inverse(my_map):
    inv_map = {}
    for k, v in my_map.items():
        inv_map[v] = inv_map.get(v, [])
        inv_map[v].append(k)
    return inv_map

# types come from main_genre_code,
# types = ['MOVIE', 'DRAMA', 'ANIME', 'VARIETY', 'MUSIC_IDOL', 'DOCUMENT', 'KIDS', 'NEWS', 'SEMIADULT']
types = ['movie', 'drama', 'anime', 'variety', 'music_idol', 'document', 'kids', 'news', 'semiadult', 'fdrama', 'adrama', 'youga', 'houga']
"""
tpye_mapping = {
    "FDRAMA":"drama",
    "ADRAMA":"drama",
    "YOUGA":"movie",
    "HOUGA":"movie"
}"""

nations_mapping = {}
nations = []

# genres come from menu_name
genres = ['action','comedy', 'drama', 'detective','documentary','fantasy','historical','horror',
          'musicals_dance', 'kids', 'mystery', 'romance', 'SF', 'family','war']
genres_mapping = {
    'アクション':'action',
    'アクション・バトル':'action',
    'コメディ':'comedy',
    'ギャグ・コメディ':'comedy',
    'ドラマ':'drama',
    '刑事・探偵':'detective',
    'ドキュメンタリー':'documentary',
    'ファンタジー':'fantasy',
    'ファンタジー・アドベンチャー':'fantasy',
    '史劇':'historical',
    '歴史・時代劇':'historical',
    'ホラー':'horror',
    'ホラー・パニック':'horror',
    'ホラー・怪談':'horror',
    'うたっておどろう♪':'musicals_dance',
    'ミュージカル・音楽':'musicals_dance',
    '音楽':'musicals_dance',
    'サスペンス・ミステリー':'mystery',
    'キッズ':'kids',
    'ラブストーリー':'romance',
    'ラブコメディ':'romance',
    'ラブロマンス':'romance',
    'ラブストーリー・ラブコメディ':'romance',
    'ロマンスシネマ':'romance',
    'ファミリー・キッズ':'family',
    '戦争':'war',
    'ミリタリー':'war',
}


# tags come from menu_name
# TODO: convert to english
tags = ['おんなのこ',
       '劇場版アニメ（国内）', '青春・学園', 'ロボット・メカ',
       'おとこのこ',  'フジテレビオンデマンド',
       'どうぶつ', 'スポーツ・競技', '日テレオンデマンド',  'ヒーロー・かいじゅう',
       'ディズニー', 'R指定', 'パラマウント',
       'ワーナーTV',  'テレビ東京オンデマンド', 'ガールズ',
       'TBSオンデマンド', 'ソニー・ピクチャーズ', 'ワーナーフィルムズ', '特撮・ヒーロー', 'クラシック',
        'ヨーロッパ',
        '20世紀FOX', '復讐・愛憎劇',  'バラエティ・K-POP', 'のりもの',
       'テレ朝動画',  '医療', '深夜放送', '任侠・ギャンブル', '劇場版アニメ（海外）', 'グラビア',
       'パチンコ・スロット', 'パチンコ・スロット・麻雀', 'バラエティ番組', 'ことば・がくしゅう', 'ボディケア・スポーツ',
        '釣り',  'お笑いライブ・ネタ', '旅', '犬猫・動物', '乗り物', '法廷',
       'アート＆カルチャー', 'アイドル・声優・タレント', '声優・舞台', '舞台・落語', '料理・グルメ',
       'NBCユニバーサル',  '風景', 'テレビ放送中', 'えほん', '教養・語学', '麻雀',
       'リアリティショー', '鉄道']




