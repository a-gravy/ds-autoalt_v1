import os, shutil, pkg_resources
import logging, os, subprocess, shlex
import numpy as np

logger = logging.getLogger(__name__)


def normalize_path(path, error_if_not_exist=False):
    if path is None:
        return path
    path = path if os.path.isabs(path) else os.path.join(os.getcwd(), path)
    if error_if_not_exist and not os.path.exists(path):
        raise Exception(f"Given path '{path}' does not exist.")
    return path


def run_command(command):
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            logger.info(output.strip().decode("utf-8"))
    rc = process.poll()
    return rc


def save_list_to_file(lst, filepath):
    with open(filepath, 'w') as f:
        ctr = 0
        for item in lst:
            f.write("%s\n" % item)
            ctr += 1


def save_str_to_file(str, filepath):
    with open(filepath, 'w') as f:
        f.write(str)


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


def get_filepath_content(path):
    source, path = path.split("://") if ":" in path else ("file", path)
    print(source, path)
    if source == "s3":
        raise Exception("Not yet supported")
    elif source == "file":
        return get_file_content_from_local_file(path)
    else:
        return get_file_content_from_module(source, path)


def send_file_to_local_dest(src, dst, delete_source_file=False):
    logger.info(f"Saving `{src}` to {dst}")
    if delete_source_file:
        shutil.move(src, dst)
    else:
        shutil.copy(src, dst)


def get_file_content_from_local_file(path):
    path if os.path.isabs(path) else os.path.join(os.getcwd(), path)
    with open(path, 'r') as f:
        content = f.read()
    return content


def get_file_content_from_module(module, resource_path):
    logger.info(f"Getting file content in module:'{module}' with given path:'{resource_path}'")
    resource_package = module
    io = pkg_resources.resource_stream(resource_package, resource_path)
    return io.read().decode('UTF-8')


def efficient_reading(input_path, with_header=True, header_format=None):
    """
    yield one line at once til go through entire file,

    & check the header_format
    """
    with open(input_path, 'r') as r:
        if with_header:
            header = r.readline().rstrip()
            logging.debug(f"reading file whose format is  {header}")
            if header_format:
                assert header == header_format, f"Header Format is WRONG"
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



def rank(model,
           target_users=None,
           target_items=None,
           filter_already_liked_items=True,
           top_n=200,  # -1 means all
           batch_size=500):
    """
    do basic reranking & filtering

    :param target_users: UID list, [uid1, uid2, ...]; None means all users in matrix
    :param target_items: SID list  [sid1, sid2, ...]; None means all items in matrix
    :param filter_already_liked_items: removed the nonzeros item in matrix, set False if you have special filtering
    :param N: minimum nb of output
    :param batch_size:
    :yield: uid, sid list, score list
    """

    # user id -> matrix index
    if target_users is None:
        target_users_index_list = list(model.user_item_matrix.user2id.values())
    else:
        target_users_index_list = [model.user_item_matrix.user2id.get(user) for user in target_users if
                                   model.user_item_matrix.user2id.get(user) is not None]

    # make target matrix, which contains target items only
    if target_items is None:
        # SID -> matrix index
        target_items2actualid = {i: i for i in range(model.user_item_matrix.matrix.shape[1])}
        target_matrix = model.user_item_matrix.matrix
        item_factors = model.item_factors
    else:
        target_items_index_list = list({model.user_item_matrix.item2id.get(item) for item in target_items if
                                        model.user_item_matrix.item2id.get(item) is not None})
        # matrix_index -> target_matrix_index
        target_items2actualid = {i: target for i, target in enumerate(target_items_index_list)}
        # target_matrix[nb of user, nb of target items], contains target items only by target_matrix_index
        target_matrix = model.user_item_matrix.matrix[:, target_items_index_list]
        item_factors = model.item_factors[target_items_index_list, :]

    # matrix operation on batch of user
    for uidxs in batch(target_users_index_list, batch_size):

        # for uidxs(a batch of user), get the score for target items,
        scores = model.user_factors[uidxs].dot(item_factors.T)
        rows = target_matrix[uidxs]

        for i, uid in enumerate(uidxs):  # for each user
            # get the top N
            nonzeros_in_row = set(rows[i].nonzero()[1]) if filter_already_liked_items else set()
            count = max(top_n + len(nonzeros_in_row), top_n*2)  # to make sure we have enough items for recommendation
            if count < len(scores[i]):  # scores[i].shape = (nb of item, )
                if top_n == -1:  # output all results
                    ids = np.argsort(scores[i])
                else:
                    ids = np.argpartition(scores[i], -count)[-count:]
                best = sorted(zip(ids, scores[i][ids]), key=lambda x: -x[1])
            else:
                best = sorted(enumerate(scores[i]), key=lambda x: -x[1])

            reranked_targer_items = [(index, score) for index, score in best if index not in nonzeros_in_row]
            score_list = [score for index, score in reranked_targer_items]
            # target matrix index -> matrix index -> item id
            reranked_item_indexs = [model.user_item_matrix.id2item[target_items2actualid[index]]
                                    for index, score in reranked_targer_items]

            yield model.user_item_matrix.id2user[uid], reranked_item_indexs, score_list

