from click import Context

from salesforce_archivist.archivist import ArchivistConfig, Archivist

import click


@click.group()
@click.pass_context
def cli(ctx: Context) -> None:
    ctx.ensure_object(dict)
    ctx.obj["config"] = ArchivistConfig("config.yaml")


@cli.command()
@click.pass_context
def download(ctx: Context) -> None:
    config: ArchivistConfig = ctx.obj["config"]
    archivist = Archivist(config)
    archivist.download()


@cli.command()
@click.pass_context
def validate(ctx: Context) -> None:
    config: ArchivistConfig = ctx.obj["config"]
    archivist = Archivist(config)
    archivist.validate()


if __name__ == "__main__":
    cli()
