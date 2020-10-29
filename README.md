# ds-auto_altmaker

https://wiki.unext-info.jp/display/DS/4.+Development+Roadmap
https://wiki.unext-info.jp/pages/viewpage.action?pageId=71446362

to automatically generate ALTs: daily_top, because you watch, new arrival


├── altmaker.py # script  
├── because_you_watched.py  
├── new_arrival.py  
├── content_based_logic.py   
├── utils.py  
├── alt_reranker.py  # rerank all alts  
├── mappings.py      # sakuhin attribute mapping, need to be rewrite  
├── sql_worker.py    # localhost version, will move to airflow DAG 
├── makefile  
└── workspace  # sql queries, keep it same as the one in airflow 

