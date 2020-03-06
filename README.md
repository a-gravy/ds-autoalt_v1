# ds-auto_altmaker

https://wiki.unext-info.jp/pages/viewpage.action?pageId=71446362

to automatically generate two main types of ALTs  
* business logic 
* content-based logic


├── altmaker.py # script to  
├── content_based_logic.py  
├── alt_reranker.py  # rerank all alts   
├── mappings.py  # sakuhin attribute mapping, need to be rewrite  
├── tmp_sql_getter.py  # localhost version, will move to airflow DAG   
└── workspace  # sql queries, keep it same as the one in airflow 

