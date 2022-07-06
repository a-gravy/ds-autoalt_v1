# ds-auto_altmaker

https://wiki.unext-info.jp/display/DS/4.+Deployment+Doc
https://wiki.unext-info.jp/pages/viewpage.action?pageId=71446362

to automatically generate ALTs: daily_top, because you watch, new arrival


├── autoalt.py # script  
├── autoalt_maker.py # base class of autoalt   
├── daily_top.py      
├── new_arrival.py   
├── because_you_watched.py    
├── genre_row_maker.py  
├── alt_reranker.py  # rerank all alts  
├── mappings.py      # sakuhin attribute mapping, need to be rewrite    
├── sql_worker.py    # localhost version, will move to airflow DAG   
└── workspace  # sql queries, keep it same as the one in airflow 

---------

functions of autoalt:
* make_alt
* black_list_filtering
* rm_duplicates
* rm_series 
* check_reco: blacklist SID, duplicates, empty

----------
### page generation (MVP)



----------
### feature allocation
allocates files to autoalt_${ippan, semiadult, adult}_features

for ippan_video: it allocates all ippan_video autoalts and feature table from 調達部:
s3://unext-datascience/starfish/keep/cassandra/unext/ippan/alt_features_implicit.csv  
-> as one file called autoalt_ippan_video_features  
-> upload to autoalt_ippan_features by workspace/alt/ippan_sakuhin/yaml/autoalt_ippan_sakuhin_features.yaml




