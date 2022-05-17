"""feature_table_checking.py

Usage:
    feature_table_checking.py checking --feature_table=PATH --target_users=PATH [--blacklist=PATH]

Options:
    --target_users PATH   active users

"""
import logging, os
from collections import defaultdict
from docopt import docopt
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)


def black_empty_duplicates_checking(feature_table_path, target_users_path, blacklist_path=None):
    blacklist = set()
    if blacklist_path:
        for line in efficient_reading(blacklist_path):
            blacklist.add(line.rstrip())
        logging.info(f"{len(blacklist)} blacklist SIDs load")
    else:
        logging.info("no black list")

    target_users = set()
    for line in efficient_reading(target_users_path):
        target_users.add(line.rstrip())
    logging.info(f"Got {len(target_users)} target users")

    user_alt_set = set()
    users_got_autoalts = set()
    # user_multi_account_id,feature_public_code,create_date,sakuhin_codes,feature_title,domain,autoalt
    for line in efficient_reading(feature_table_path,
                                  header_format="user_multi_account_id,feature_public_code,create_date,sakuhin_codes,feature_title,domain,autoalt,feature_public_start_datetime,feature_public_end_datetime"):
        arr = line.split(",")
        user_alt = f"{arr[0]}_{arr[1]}"

        # duplicates ALT checking
        assert user_alt not in user_alt_set, f"ERROR : duplicates {user_alt}"
        user_alt_set.add(user_alt)

        if "JFET" in line:  # only for AutoALTs
            users_got_autoalts.add(arr[0])

            # SIDs checking
            sid_pool = set()
            SIDs = line.split(",")[3].split("|")
            assert SIDs, f"ERROR, empty reco in {line}"
            for sid in SIDs:
                if blacklist and arr[1] != "JFET000003":  # except for New arrivals EP(not reco, just a useful ALT), useful for binge-watching NHK Dramas users
                    assert sid not in blacklist, f"ERROR, black SID {sid} in {line}"

                assert sid not in sid_pool, f"ERROR, duplicates SID:{sid} in {line}"
                sid_pool.add(sid)

    assert len(target_users) >= len(users_got_autoalts), \
        f"ERROR nb of target users:{len(target_users)} < nb of users got autoalts:{len(users_got_autoalts)}"

    logging.info(f"{len(users_got_autoalts)} users got autoalts < nb of target users:{len(target_users)}, pass")
    logging.info(f"black_empty_duplicates_checking checking pass, good to go")


if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.1.0')
    logging.info(arguments)

    if arguments['checking']:
        black_empty_duplicates_checking(arguments["--feature_table"], arguments["--target_users"],
                                        arguments.get("--blacklist", None))
    #elif arguments['nb_users_checking']:
    #    nb_users_checking(arguments["--page_table"], arguments["--target_users"])
