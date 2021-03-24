import os, shutil, pkg_resources
import logging, os, subprocess, shlex

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



