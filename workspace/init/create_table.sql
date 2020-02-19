create table alt.dim_menu_category (
  menu_public_code varchar,
  menu_name varchar,
  category_name varchar,
  category_id varchar,
  update_time timestamp
)


insert into alt.dim_menu_category values
('MNU0000071','アクション','action','CAT0001',now()),
('MNU0000065','ラブロマンス','romance','CAT0002',now()),
('MNU0000078','サスペンス・ミステリー','mystery','CAT0003',now()),
('MNU0000079','コメディ','comedy','CAT0004',now()),
('MNU0000064','ラブコメディ','romance','CAT0002',now()),
('MNU0000039','コメディ','comedy','CAT0004',now()),
('MNU0000038','サスペンス・ミステリー','mystery','CAT0003',now()),
('MNU0000009','ファンタジー','fantasy','CAT0005',now()),
('MNU0000030','ドキュメンタリー','documentary','CAT0006',now()),
('MNU0000005','ラブコメディ','romance','CAT0002',now()),
('MNU0000037','アクション','action','CAT0001',now()),
('MNU0000003','歴史・時代劇','historical','CAT0007',now()),
('MNU0000023','コメディ','comedy','CAT0004',now()),
('MNU0000014','ラブストーリー','romance','CAT0002',now()),
('MNU0000021','SF','SF','CAT0008',now()),
('MNU0000052','アクション','action','CAT0001',now()),
('MNU0000011','アクション・バトル','action','CAT0001',now()),
('MNU0000029','史劇','historical','CAT0007',now()),
('MNU0000028','戦争','war','CAT0009',now()),
('MNU0000007','SF','SF','CAT0008',now()),
('MNU0000045','ドキュメンタリー','documentary','CAT0006',now()),
('MNU0000040','ラブストーリー・ラブコメディ','romance','CAT0002',now()),
('MNU0000081','ホラー・パニック','horror','CAT0010',now()),
('MNU0000080','ラブストーリー・ラブコメディ','romance','CAT0002',now()),
('MNU0000041','ホラー・パニック','horror','CAT0010',now()),
('MNU0000103','音楽','music','CAT0011',now()),
('MNU0000048','ロマンスシネマ','romance','CAT0002',now()),
('MNU0000104','ドキュメンタリー','documentary','CAT0006',now()),
('MNU0000057','ホラー','horror','CAT0010',now()),
('MNU0000008','サスペンス・ミステリー','mystery','CAT0003',now()),
('MNU0000010','ミリタリー','war','CAT0009',now()),
('MNU0000042','歴史・時代劇','historical','CAT0007',now()),
('MNU0000027','ミュージカル・音楽','music','CAT0011',now()),
('MNU0000056','ラブストーリー・ラブコメディ','romance','CAT0002',now()),
('MNU0000097','うたっておどろう♪','music','CAT0011',now()),
('MNU0000086','歴史・時代劇','historical','CAT0007',now()),
('MNU0000024','ラブストーリー・ラブコメディ','romance','CAT0002',now()),
('MNU0000055','コメディ','comedy','CAT0004',now()),
('MNU0000025','ホラー・パニック','horror','CAT0010',now()),
('MNU0000026','ファンタジー・アドベンチャー','fantasy','CAT0005',now()),
('MNU0000084','刑事・探偵','detective','CAT0012',now()),
('MNU0000068','コメディ','comedy','CAT0004',now()),
('MNU0000070','サスペンス・ミステリー','mystery','CAT0003',now()),
('MNU0000053','SF','SF','CAT0008',now()),
('MNU0000069','歴史・時代劇','historical','CAT0007',now()),
('MNU0000020','アクション','action','CAT0001',now()),
('MNU0000015','ファミリー・キッズ','family','CAT0013',now()),
('MNU0000022','サスペンス・ミステリー','mystery','CAT0003',now()),
('MNU0000054','サスペンス・ミステリー','mystery','CAT0003',now()),
('MNU0000059','刑事・探偵','detective','CAT0012',now())


create table alt.dim_sakuhin_category
(
  sakuhin_category_id varchar,
  sakuhin_public_code varchar,
  sakuhin_name varchar,
  main_genre_code varchar,
  category_id varchar,
  category_name varchar
)


/*insert into alt.dim_sakuhin_category (
select
  distinct
  sakuhin_public_code || category_id as sakuhin_category_id,
  sakuhin_public_code,
  dsm.display_name,
  main_genre_code,
  category_name,
  category_id
from dim_sakuhin_menu dsm
inner join dim_sakuhin using(sakuhin_public_code)
inner join alt.dim_menu_category using(menu_public_code)
)*/
