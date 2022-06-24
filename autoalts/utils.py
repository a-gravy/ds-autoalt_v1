"""
utils

Usage:
    utils.py unzip_dir --input=PATH
    utils.py demo_candidates --input=PATH
    utils.py pfid_div --input=PATH
    utils.py target_users --input=PATH [ --superusers=PATH ] [<nb>...]
    utils.py tmp

Options:
    -h --help Show this screen
    --version
    --input PATH          File or dir path of input
    --output PATH         File or dir path of output
    --model PATH          File path location of the trained model
"""
import os
import logging
import gzip
import shutil
import yaml
import boto3
import pickle
import time
from google.cloud import storage
from docopt import docopt

logging.basicConfig(level=logging.INFO)

with open("config.yaml") as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)


def efficient_reading(input_path, with_header=True, header_format=None):
    """
    yield one line at once til go through entire file,

    & check the header_format
    """
    with open(input_path, 'r') as r:
        if with_header:
            header = r.readline().rstrip()
            logging.info(f"reading file whose format is  {header}")
            if header_format:
                assert header == header_format, f"Header Format:{header} is WRONG"
        while True:
            line = r.readline()
            if line:
                yield line
            else:
                break


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def load_model(path):
    return pickle.load(open(path, "rb"))


def pfid_divider(input_path="data/user_pfid.csv", superusers_path="data/superusers.csv"):
    """
    read  pfid_matsubi,user_platform_id,user_multi_account_id

    divide users into 10 groups by the end nb of pfid
    * append superusers at front
    * output as pfid_{i}_users.csv

    :param input_path:
    :return:
    """
    pfid_writters = []
    for i in range(10):
        writer = open(f'pfid_{i}_users.csv', 'w')
        writer.write("user_multi_account_id\n")
        pfid_writters.append(writer)

    for line in efficient_reading(superusers_path):  # write superusers also
        for i in range(10):
            pfid_writters[i].write(line)

    for line in efficient_reading(input_path):
        arr = line.rstrip().split(',')
        pfid_writters[int(arr[0])].write(f"{arr[2]}\n")

    for i in range(10):
        pfid_writters[i].close()

    logging.info("output files = ", os.listdir("."))


def file_to_list(filepath, ignore_header=False):
    lst = []
    ctr = 0
    with open(filepath, 'r') as f:
        for line in f:
            ctr += 1
            if ctr == 1 and ignore_header: continue
            item = line[:-1].strip()
            if item != "":
                lst.append(item)
    return lst


def split_s3_path(text):
    """
    split s3://unext-datascience-prod/jobs/ippanreco/
    return unext-datascience-prod(as bucket), jobs/ippanreco(as key)
    """
    arr = text.split("/")
    arr = [x for x in arr if x]
    return arr[1], '/'.join(arr[2:])

def get_files_from_gcs(**kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket("ds-airflow-jobs")
    for k, v in kwargs.items():
        if not isinstance(v, str):
            continue
        # data/xxx.csv -> xxx.csv -> xxx.csv.gz
        # data/yyy.model -> yyy.model
        if os.path.exists(v):
            print(f"{v} exists")
            continue

        if v.endswith(".csv"):
            file_name = os.path.basename(v)
            print(f"download {file_name}")
            file_name = f'{file_name}.gz' if v.endswith(".csv") else file_name  # csv files in s3 is zipped
            blob = bucket.blob(f"autoalt_v1/dev/{file_name}")
            blob.download_to_filename(f"data/{file_name}")


def get_files_from_cloud(**kwarg):
    env = "prod"
    logging.info(f"Downloading Files (env:{env})")
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket("ds-airflow-jobs")
    aws_client = boto3.client('s3')

    for k, v in kwarg.items():
        if not isinstance(v, str):
            continue
        # data/xxx.csv -> xxx.csv -> xxx.csv.gz
        # data/yyy.model -> yyy.model
        if os.path.exists(v):
            print(f"{v} exists")
            continue

        file_name = os.path.basename(v)
        if v.endswith(".csv"):
            print(f"download {file_name}")
            file_name = f'{file_name}.gz' if v.endswith(".csv") else file_name  # csv files in s3 is zipped
            blob = gcs_bucket.blob(f"autoalt_v1/{env}/{file_name}")
            blob.download_to_filename(f"data/{file_name}")
        elif 'model' in v:
            s3_dir_path = "s3://unext-datascience-prod/jobs/collaborative/ippan_gke/"
            s3_bucket, key = split_s3_path(s3_dir_path)
            print(f"downloading {s3_bucket} {key}/{file_name}")
            with open(f"data/{file_name}", 'wb') as f:
                aws_client.download_fileobj(s3_bucket, f"{key}/{file_name}", f)


def get_files_from_s3(domain_name, **kwarg):
    """
    download all files from s3
    :param domain_name: e.g. ippan_sakuhin
    """
    client = boto3.client('s3')

    for k, v in kwarg.items():
        if not isinstance(v, str):
            continue

        # data/xxx.csv -> xxx.csv -> xxx.csv.gz
        # data/yyy.model -> yyy.model
        if os.path.exists(v):
            print(f"{v} exists")
            continue

        file_name = os.path.basename(v)

        if 'model' in v:  # file name w/ DATE
            s3_dir_path = config['s3_dir']['model_root']

            bucket, key = split_s3_path(s3_dir_path)
            print(f"downloading {bucket} {key}/{file_name}")
            with open(f"data/{file_name}", 'wb') as f:
                client.download_fileobj(bucket, f"{key}/{file_name}", f)
        elif v.endswith(".csv") or v.endswith(".pkl"):
            s3_dir_path = config['s3_dir'].get(file_name, None)
            if s3_dir_path:  # special s3 path
                bucket, key = split_s3_path(s3_dir_path)
            else:   # others = domain bucket
                s3_dir_path = config['s3_dir'][domain_name]
                bucket, key = split_s3_path(s3_dir_path)

            file_name = f'{file_name}.gz' if v.endswith(".csv") else file_name  # csv files in s3 is zipped
            print(f"downloading {bucket} {key}/{file_name}")

            with open(f"data/{file_name}", 'wb') as f:
                client.download_fileobj(bucket, f"{key}/{file_name}", f)


def unzip_files_in_dir(dir_path):
    """
    unzip all file and save them in the same dir

    :param dir_path: str
    :return:
    """
    for file in os.listdir(dir_path):
        if file.endswith(".gz"):
            start_t = time.time()
            logging.info(f"unzipping {file}")
            with gzip.open(os.path.join(dir_path, file), 'rb') as f_in:
                name_without_gz = os.path.splitext(file)[0]
                with open(os.path.join(dir_path, name_without_gz), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(os.path.join(dir_path, file))
            logging.info(f"{file} takes {time.time() - start_t} to unzip")
        else:
            logging.info(f"skip {file}")


def make_demo_candidates(feature_table_path, output_path="demo_candidates.csv"):
    set_dict = {"JFET000002": set(), "JFET000003": set()}

    with open(feature_table_path, "r") as r:
        r.readline()
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().split(",")
                feature_public_code = arr[1]
                if set_dict.get(feature_public_code, "no found") != "no found":  # for set()
                    set_dict[feature_public_code].add(arr[0])
            else:
                break

    for k, v in set_dict.items():
        logging.info(f"{k} has {len(v)} users")

    logging.info(f"JFET000002 & JFET000003 = {len(set_dict['JFET000002'] & set_dict['JFET000003'])}")

    cnt = 0
    with open(output_path, "w") as w:
        w.write("user_multi_account_id\n")
        for user_id in (set_dict['JFET000002'] & set_dict['JFET000003']):
            w.write(f"{user_id}\n")
            cnt += 1
            if cnt > 100:
                break

        if cnt < 100:
            for user_id in (set_dict['JFET000003'] - set_dict['JFET000002']):
                w.write(f"{user_id}\n")
                cnt += 1
                if cnt > 100:
                    break


def toppick_rm_series(series_path, input, output, target_users_path):
    """
    input a toppick file, output a version with rm series

    :param input:   format: user_multi_account_id,platform,block,sakuhin_codes,feature_name,sub_text,score,create_date
    :param output:  format: user_multi_account_id,block,sakuhin_codes,feature_name,sub_text,score,create_date
    :return:
    """
    target_users = set()
    with open(target_users_path, "r") as r:
        for line in r.readlines():
            target_users.add(line.rstrip())

    series_dict = {}
    with open(series_path, "r") as r:  # sakuhin_public_code,series_id,series_in_order
        print(r.readline())
        while True:
            line = r.readline()
            if line:
                arr = line.rstrip().replace('"', '').split(",")
                if arr[1]:
                    series_dict[arr[0]] = arr[1]
            else:
                break
    logging.info(f"read {len(series_dict)} dict(sid:series_id) done ")

    def rm_series_str(SIDs):
        series_pool = set()
        reco_item_list = []
        for sid in SIDs:
            series_id = series_dict.get(sid, None)
            if series_id:
                if series_id in series_pool:
                    continue
                else:
                    series_pool.add(series_id)
                    reco_item_list.append(sid)
            else:
                reco_item_list.append(sid)
        if len(SIDs) - len(reco_item_list) != 0:
            logging.debug(f"[rm_series] from {len(SIDs)} to {len(reco_item_list)}")

        return reco_item_list

    with open(output, "w") as w:
        w.write("user_multi_account_id,block,sakuhin_codes,feature_name,sub_text,score,create_date\n")
        with open(input, "r") as r:
            r.readline()
            while True:
                line = r.readline()
                if line:
                    arr = line.rstrip().split(",")
                    if arr[0] in target_users:
                        SIDs_rms = rm_series_str(arr[3].split("|"))
                        if len(SIDs_rms) < 3:
                            continue
                        w.write(f"{arr[0]},1,{'|'.join(SIDs_rms)},,,{'|'.join(arr[-2].split('|')[:len(SIDs_rms)])},{arr[-1]}\n")
                else:
                    break


def make_target_users(pfid_path, target_pfid, superusers_path=None):
    """
    pfid_path with format:
        pfid_matsubi,user_platform_id,user_multi_account_id
        4,32464014,PM029911034

    target_pfid: list of nb e.g. ['5', '6']

    if superusers_path: superusers are inside target_users

    :return:
    """
    target_pfid = set(target_pfid)
    user_cnt = 0
    user_set = set()  # for preventing duplicates

    if superusers_path:
        for line in efficient_reading(superusers_path):
            user_set.add(line.rstrip())

    for line in efficient_reading(pfid_path, header_format="user_platform_id,multi_account_id"):
        arr = line.rstrip().split(',')
        last_pfid = arr[0][-1]
        user_id = arr[-1]
        if last_pfid in target_pfid:
            user_set.add(user_id)

    with open("target_users.csv", "w") as w:
        w.write("user_multi_account_id\n")
        for user_id in user_set:
            w.write(f"{user_id}\n")
            user_cnt += 1

    logging.info(f"We have {user_cnt} target users")


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    # read dim_autoalt.csv
    if arguments['unzip_dir']:
        unzip_files_in_dir(arguments['--input'])
    elif arguments['pfid_div']:
        pfid_divider(arguments['--input'])
    elif arguments["target_users"]:
        # superusers + pfid 1,2,3 = python autoalts/utils.py target_users --superusers data/superusers.csv   --input data/user_pfid.csv 1 2 3
        # superusers only = python autoalts/utils.py target_users --superusers data/superusers.csv   --input data/user_pfid.csv 1 2 3
        make_target_users(pfid_path=arguments['--input'], target_pfid=arguments['<nb>'],
                          superusers_path=arguments["--superusers"])
    elif arguments['tmp']:
        kwargs = {
            "x":"user_sid_history.csv",
            #"z":"popular.csv",
            #"m": "als_model.latest"
        }
        get_files_from_cloud(**kwargs)
    else:
        raise Exception("Unimplemented ERROR")


if __name__ == '__main__':
    main()
