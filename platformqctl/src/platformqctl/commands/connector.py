import click
import httpx
import json

def handle_api_error(response: httpx.Response):
    if response.status_code >= 400:
        click.echo(f"Error: API returned status {response.status_code} - {response.text}")
    response.raise_for_status()

@click.group("connector")
def connector_cli():
    """Commands for interacting with the Connector Service."""
    pass

@connector_cli.command("list")
@click.pass_context
def list_connectors(ctx):
    """Lists all available connector plugins."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/connector-service/connectors"
    
    click.echo("Fetching available connectors...")
    try:
        with httpx.Client() as client:
            response = client.get(base_url)
            handle_api_error(response)
            click.echo(json.dumps(response.json(), indent=2))
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError:
        pass

@connector_cli.command("run")
@click.argument("connector_name")
@click.option("--context", help="The context payload as a JSON string.")
@click.pass_context
def run_connector(ctx, connector_name: str, context: str):
    """
    Manually triggers a run for a specific connector.
    
    Example for a file-based connector:
    platformqctl connector run blender --context '{"file_uri": "models/my_model.blend"}'
    """
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/connector-service/connectors"
    
    try:
        context_json = json.loads(context) if context else {}
    except json.JSONDecodeError:
        click.echo("Error: --context must be a valid JSON string.", err=True)
        return

    click.echo(f"Triggering run for connector '{connector_name}'...")
    try:
        with httpx.Client() as client:
            response = client.post(f"{base_url}/{connector_name}/run", json=context_json)
            handle_api_error(response)
            click.echo(json.dumps(response.json(), indent=2))
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError:
        pass 