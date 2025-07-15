import click
import httpx

# Configuration would come from a config file
SIMULATION_SERVICE_URL = "http://localhost:8004/api/v1/simulations" 

# Re-using the helper from the assets command would be ideal in a real app
def handle_api_error(response: httpx.Response):
    if response.status_code >= 400:
        click.echo(f"Error: API returned status {response.status_code} - {response.text}")
    response.raise_for_status()

@click.group("simulation")
def simulation_cli():
    """Commands for interacting with the Simulation Engine."""
    pass

@simulation_cli.command("list")
@click.pass_context
def list_simulations(ctx):
    """Lists all available simulations."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/simulation-service/api/v1/simulations"

    click.echo("Fetching simulations from the simulation-service...")
    try:
        with httpx.Client() as client:
            response = client.get(base_url)
            handle_api_error(response)
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError:
        # The helper function already printed the error
        pass

@simulation_cli.command("run")
@click.argument("simulation_id")
@click.pass_context
def run_simulation(ctx, simulation_id: str):
    """Triggers a new run for a specific simulation."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/simulation-service/api/v1/simulations"

    click.echo(f"Requesting new run for simulation {simulation_id}...")
    
    # In a real app, this would likely be a POST request
    # e.g., client.post(f"{base_url}/{simulation_id}/run")
    click.echo("(This is a placeholder - a real implementation would POST to the service)") 