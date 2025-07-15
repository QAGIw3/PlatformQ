import click
import os
from ruamel.yaml import YAML

# Import command groups
from .commands.create_service import service_cli
from .commands.assets import assets_cli
from .commands.events import events_cli
from .commands.simulation import simulation_cli
from .commands.trust import trust_cli
from .commands.intelligence import intelligence_cli
from .commands.lakehouse import lakehouse_cli
from .commands.connector import connector_cli

CONFIG_FILE_NAME = ".platformqctl.yaml"
yaml = YAML()

# --- Main CLI Group ---
@click.group()
@click.pass_context
def cli(ctx):
    """
    A CLI tool for managing the platformQ ecosystem.
    
    This tool provides commands for scaffolding services, interacting with
    Digital Assets, and monitoring the event bus.
    """
    ctx.ensure_object(dict)
    ctx.obj['config'] = load_config()

# --- Helper Functions ---
def load_config():
    if not os.path.exists(CONFIG_FILE_NAME):
        return None
    with open(CONFIG_FILE_NAME, 'r') as f:
        return yaml.load(f)

# --- Top-level Commands ---
@cli.command()
@click.pass_context
def init(ctx):
    """Initializes a new config file for platformqctl."""
    config_data = {
        "enterprise": {
            "registry_url": "gcr.io/your-company",
            "helm_repo": "https://charts.your-company.com",
        },
        "api_gateway_url": "https://api.platformq.your-domain.com",
        "trino_host": "trino.platformq.your-domain.com",
        "pulsar_url": "pulsar://pulsar.platformq.your-domain.com:6650",
    }
    with open(CONFIG_FILE_NAME, 'w') as f:
        yaml.dump(config_data, f)
    click.echo(f"Initialized config file at ./{CONFIG_FILE_NAME}")


# --- Register Command Groups ---
cli.add_command(service_cli)
cli.add_command(assets_cli)
cli.add_command(events_cli)
cli.add_command(simulation_cli)
cli.add_command(trust_cli)
cli.add_command(intelligence_cli)
cli.add_command(lakehouse_cli)
cli.add_command(connector_cli)

if __name__ == '__main__':
    cli(obj={}) 