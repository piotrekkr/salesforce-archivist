import signal
from types import FrameType

import click
import yaml
from click import Context

from salesforce_archivist.archivist import Archivist, ArchivistConfig
from simple_salesforce import Salesforce as SalesforceClient


def signal_handler(signum: int, frame: FrameType | None) -> None:
    print("Received signal {}. Attempting graceful shutdown. Please wait...".format(signum))
    raise KeyboardInterrupt


signal.signal(signal.SIGINT, signal_handler)


@click.group()
@click.pass_context
def cli(ctx: Context) -> None:
    ctx.ensure_object(dict)
    with open("config.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    ctx.obj["config"] = ArchivistConfig(**config)


@cli.command()
@click.option("--validate", is_flag=True, default=False, help="Trigger validation after download.")
@click.pass_context
def download(ctx: Context, validate: bool) -> None:
    config: ArchivistConfig = ctx.obj["config"]
    sf_client = SalesforceClient(
        instance_url=config.auth.instance_url,
        domain=config.auth.domain,
        username=config.auth.username,
        consumer_key=config.auth.consumer_key,
        privatekey=config.auth.private_key,
    )
    archivist = Archivist(
        data_dir=config.data_dir,
        objects=config.objects,
        sf_client=sf_client,
        max_api_usage_percent=config.max_api_usage_percent,
        max_workers=config.max_workers,
    )
    if not archivist.download() or validate and not archivist.validate():
        ctx.exit(code=1)


@cli.command()
@click.pass_context
def validate(ctx: Context) -> None:
    config: ArchivistConfig = ctx.obj["config"]
    sf_client = SalesforceClient(
        instance_url=config.auth.instance_url,
        domain=config.auth.domain,
        username=config.auth.username,
        consumer_key=config.auth.consumer_key,
        privatekey=config.auth.private_key,
    )
    archivist = Archivist(
        data_dir=config.data_dir,
        objects=config.objects,
        sf_client=sf_client,
        max_api_usage_percent=config.max_api_usage_percent,
        max_workers=config.max_workers,
    )
    if not archivist.validate():
        ctx.exit(code=1)


if __name__ == "__main__":
    cli()
