import os, logging
import rich_click as click
from non_acc_metrics import CatalogCoverage, Novelty, Diversity, Serendipity
#from dstools.connectors.bigquery import Uploader
from tobq import BigQuery
logging.basicConfig(level=logging.INFO)

@click.command()
@click.option('--black_list_path', help='csv of black list SIDs')
@click.argument('reco_path')
@click.argument('model_path')
def catalog_coverage(reco_path, model_path, black_list_path):
    click.echo(f"catalog_coverage: {reco_path} {model_path} {black_list_path}")
    cc = CatalogCoverage(model_path, black_list_path)
    cc.calculate(reco_path)

@click.command()
@click.option('--black_list_path', help='csv of black list SIDs')
@click.argument('reco_path')
@click.argument('training_data_path')
def novelty(reco_path, training_data_path, black_list_path):
    click.echo(f"novelty : {reco_path} {training_data_path} {black_list_path} ")
    n = Novelty(training_data_path, black_list_path)
    n.calculate(reco_path)

@click.command()
@click.option('--black_list_path', help='csv of black list SIDs')
@click.argument('reco_path')
@click.argument('training_data_path')
def novelty(reco_path, training_data_path, black_list_path):
    click.echo(f"novelty : {reco_path} {training_data_path} {black_list_path} ")
    n = Novelty(training_data_path, black_list_path)
    n.calculate(reco_path)

@click.command()
@click.argument('reco_path')
@click.argument('model_path')
@click.argument('sakuhin_meta_path')
def diversity(model_path, reco_path, sakuhin_meta_path):
    click.echo(f"diversity : {reco_path} {model_path} {sakuhin_meta_path} ")
    metric = Diversity(model_path, sakuhin_meta_path)
    metric.calculate(reco_path)

@click.command()
@click.argument('reco_path')
@click.argument('model_path')
@click.argument('sakuhin_meta_path')
@click.argument('history_path')
def serendipity(model_path, reco_path, sakuhin_meta_path, history_path):
    click.echo(f"serendipity : {reco_path} {model_path} {sakuhin_meta_path} {history_path}")
    metric = Serendipity(model_path, sakuhin_meta_path, history_path)
    metric.calculate(reco_path)


@click.command()
@click.argument('csv_path')
@click.argument('table_id')
def tobq(csv_path, table_id):
    click.echo(f"tobq : {csv_path} {table_id}")
    bq = BigQuery()
    bq.upload_csv(csv_path=csv_path,
                  table_id=table_id,
                  string_cols=["page", "metric_name"],
                  parse_date_cols=["dt"])
    # "un-ds-dwh.model_metrics.ippan_non_accuracy_metrics"

@click.group()
def cli():
    pass


cli.add_command(catalog_coverage)
cli.add_command(novelty)
cli.add_command(diversity)
cli.add_command(serendipity)
cli.add_command(tobq)

if __name__ == '__main__':
    cli()
