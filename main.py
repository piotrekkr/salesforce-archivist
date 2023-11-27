from salesforce_archivist.archivist import ArchivistConfig, Archivist

import click


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    ctx.obj["config"] = ArchivistConfig("config.yaml")


@cli.command()
@click.pass_context
def download(ctx):
    config: ArchivistConfig = ctx.obj["config"]
    archivist = Archivist(config)
    archivist.download()
    click.echo("Download finished!")


@cli.command()
@click.pass_context
def validate(ctx):
    click.echo("Validate")


if __name__ == "__main__":
    cli()
