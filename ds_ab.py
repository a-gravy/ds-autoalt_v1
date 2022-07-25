import os, logging
import rich_click as click
import hashlib
import logging
from ab_testing import user_group_split
from autoalts.utils import efficient_reading

logging.basicConfig(level=logging.INFO)



@click.command()
@click.argument('target_users_path')
@click.argument('experiment')
@click.option('--groups', '-g', multiple=True)
@click.argument('super_users_group')
def group_split(target_users_path: str, experiment: str, groups: list, super_users_group):
    logging.info(f"groups = {groups}")
    user_group_split(target_users_path, experiment, list(groups), super_users_group)


@click.command()
@click.argument('output_path')
@click.option('--files', '-f', multiple=True)
def append_files(output_path: str, files: list):
    with open(output_path, "w") as w:
        # for toppick header
        w.write("user_multi_account_id,sakuhin_codes,create_date,feature_name,sub_text,block\n")
        for file in files:
            logging.info(f"processing {file}")
            for line in efficient_reading(file):
                w.write(line)


@click.group()
def cli():
    pass


cli.add_command(group_split)
cli.add_command(append_files)


if __name__ == '__main__':
    cli()
