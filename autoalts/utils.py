"""
utils

Usage:
    utils.py unzip_dir --input=PATH
    utils.py demo_candidates --input=PATH

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
from docopt import docopt

logging.basicConfig(level=logging.INFO)

with open("config.yaml") as f:
    config = yaml.load(f.read(), Loader=yaml.FullLoader)


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
        file_name = os.path.basename(v)

        if '.model' in v:  # file name w/ DATE
            s3_dir_path = config['s3_dir']['ippan_bpr_root']
            bucket, key = split_s3_path(s3_dir_path)
            print(f"downloading {bucket} {key}/{file_name}")

            with open(f"data/{file_name}", 'wb') as f:
                client.download_fileobj(bucket, f"{key}/{file_name}", f)
                # client.download_fileobj("unext-datascience-prod", f"jobs/ippanreco/{file_name}", f)
        elif 'postplay_implicit' in v:
            s3_dir_path = config['s3_dir']['ippan_bpr_root']
            bucket, key = split_s3_path(s3_dir_path)

            file_name = f'{file_name}.gz'  # the file in s3 is zipped
            print(f"downloading {bucket} {key}/{file_name}")

            with open(f"data/{file_name}", 'wb') as f:
                client.download_fileobj(bucket, f"{key}/{file_name}", f)
        elif v.endswith(".csv"):
            s3_dir_path = config['s3_dir'].get(file_name, None)
            if s3_dir_path:  # special s3 path
                bucket, key = split_s3_path(s3_dir_path)
            else:   # others = domain bucket
                s3_dir_path = config['s3_dir'][domain_name]
                bucket, key = split_s3_path(s3_dir_path)

            file_name = f'{file_name}.gz'  # the file in s3 is zipped
            print(f"downloading {bucket} {key}/{file_name}")

            with open(f"data/{file_name}", 'wb') as f:
                client.download_fileobj(bucket, f"{key}/{file_name}", f)
        else:
            pass


def unzip_files_in_dir(dir_path):
    """
    unzip all file and save them in the same diretory

    :param dir_path: str
    :return:
    """
    for file in os.listdir(dir_path):
        if file.endswith(".gz"):
            logging.info(f"unzipping {file}")
            with gzip.open(os.path.join(dir_path, file), 'rb') as f_in:
                name_without_gz = os.path.splitext(file)[0]
                with open(os.path.join(dir_path, name_without_gz), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(os.path.join(dir_path, file))
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


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    # read dim_autoalt.csv
    if arguments['unzip_dir']:
        unzip_files_in_dir(arguments['--input'])
    else:
        raise Exception("Unimplemented ERROR")



if __name__ == '__main__':
    main()