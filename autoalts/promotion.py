"""
promotion

Usage:
    promotion.py rizin feature --target_users=PATH <g1s> <g1e> <g2s> <g2e>
    promotion.py rizin page --target_users=PATH <g1s> <g1e> <g2s> <g2e>

Options:
    -h --help
    --version
    --target_users PATH       the start time of the event
"""
import os
import logging
from docopt import docopt
from utils import efficient_reading
import datetime
import pytz

logging.basicConfig(level=logging.INFO)

# coldstart: 12/25 00:00 to 1/1 23:59
# G2: 12/25 00:00〜 1/4 23:59
G1_time = {"start": '2021-12-25', "end":'2022-01-01'}
G2_time = {"start": '2021-12-23', "end":'2022-12-24'}  #{"start":'2021-12-25',"end":'2022-01-04'}

jp_tz = pytz.timezone('Asia/Tokyo')
jp_datetime = datetime.datetime.now(jp_tz).isoformat()
logging.info(f"execution at {datetime.datetime.now()} server time, convert to {datetime.datetime.now(jp_tz) } Japan timezone")

promotion_ALT_content = "FET0010175,20210810,SID0062737|SID0062125|SID0062093|SID0063225|SID0063955|SID0062095|SID0062096|SID0062090|SID0062233|SID0062228|SID0062124|SID0062122|SID0062232|SID0062237|SID0062092|SID0062094|SID0063956|SID0062091|SID0062231|SID0062282|SID0062244|SID0062281|SID0062123|SID0062283|SID0062560|SID0062279|SID0062242|SID0062286|SID0062245|SID0062241|SID0062285|SID0062278|SID0062280|SID0062276|SID0062243|SID0062284|SID0062277,,ippan_sakuhin,0,2021-10-02 11:00:00,2023-10-01 23:59:59"


def rizin_features(promotion_ALT, target_users):
    """
    if target user X has no promotion_ALT, add a line "X,promotion_ALT..."

    :param promotion_ALT:
    :param target_users:
    :return:
    """
    if jp_datetime >= G1_time["start"] and jp_datetime <= G1_time["end"]:
        logging.info(f"rizin_features G1 processing")
        target_users.add("coldstart")
    else:
        logging.info(f"rizin_features G1 is inactive ")

    if jp_datetime >= G2_time["start"] and jp_datetime <= G2_time["end"]:
        logging.info(f"rizin_features G2 processing")
        for line in efficient_reading("data/autoalt_ippan_sakuhin_features.csv"):
            arr = line.split(",")
            if arr[1] == promotion_ALT and arr[0] in target_users:
                target_users.remove(arr[0])

        with open("data/autoalt_ippan_sakuhin_features.csv", "a") as a:
            for user_id in target_users:
                a.write(f"{user_id},{promotion_ALT_content}\n")
    else:
        logging.info(f"rizin_features G2 is inactive ")


def rizin_page(promotion_ALT, target_users):
    """
    for target users & coldstart:
        remove promotion_ALT in the ALT list
        push promotion_ALT to the 1st place

    :param promotion_ALT:
    :param target_users:
    :return:
    """
    output = open("data/autoalt_MAINPAGE_page_promotion.csv", "w")
    output.write("user_multi_account_id,page_public_code,create_date,feature_public_codes\n")

    if jp_datetime >= G1_time["start"] and jp_datetime <= G1_time["end"]:
        G1_active = True
        logging.info("rizin_page G1 is activate")
    else:
        G1_active = False
        logging.info("rizin_page G1 is inactivate")

    if jp_datetime >= G2_time["start"] and jp_datetime <= G2_time["end"]:
        G2_active = True
        logging.info("rizin_page G2 is activate")
    else:
        G2_active = False
        logging.info("rizin_page G2 is inactivate")

    for line in efficient_reading("data/autoalt_MAINPAGE_page.csv"):
        arr = line.split(",")

        if arr[0] == "coldstart" and arr[1] == "MAINPAGE" and G1_active:
            arr[3] = arr[3].replace(f"{promotion_ALT}|", "")
            arr[3] = f"{promotion_ALT}|{arr[3]}"
            output.write(",".join(arr))
        elif arr[0] == "coldstart" and arr[1] == "MAINPAGE" and not G1_active:
            logging.info(f"coldstart = {line[:100]}")
            output.write(line)
        elif arr[0] != "coldstart" and arr[1] == "MAINPAGE" and G2_active:
            if arr[0] in target_users:
                arr[3] = arr[3].replace(f"{promotion_ALT}|", "")
                arr[3] = f"{promotion_ALT}|{arr[3]}"
                output.write(",".join(arr))
            else:
                output.write(line)
        else:
            output.write(line)


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)

    logging.info("version 1.1.2")
    G1_time["start"] = arguments['<g1s>']
    G1_time["end"] = arguments['<g1e>']
    G2_time["start"] = arguments['<g2s>']
    G2_time["end"] = arguments['<g2e>']
    logging.info(f"activation time of G1 = {G1_time}")
    logging.info(f"activation time of G2 = {G2_time}")

    target_users = set()
    for line in efficient_reading(arguments["--target_users"]):
        # for superusers, there is only one field
        # for rizin_provisional_target_list.csv: "user_multi_account_id","model_similarity","battle_content_watched","watched_live","search_user","is_superuser"
        arr = line.rstrip().split(",")
        target_users.add(arr[0])

    logging.info(f"we have {len(target_users)} target users, e.g. {list(target_users)[:3]}")


    promotion_ALT = "FET0010175"

    if arguments["feature"]:
        rizin_features(promotion_ALT, target_users)
    elif arguments["page"]:
        rizin_page(promotion_ALT, target_users)

    logging.info("Done")


if __name__ == '__main__':
    main()