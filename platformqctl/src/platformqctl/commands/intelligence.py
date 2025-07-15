import click
import httpx
import json

# Configuration would come from a config file
INTELLIGENCE_SERVICE_URL = "http://localhost:8006/api/v1/insights" 

def handle_api_error(response: httpx.Response):
    if response.status_code >= 400:
        click.echo(f"Error: API returned status {response.status_code} - {response.text}")
    response.raise_for_status()

@click.group("intelligence")
def intelligence_cli():
    """Commands for the Graph Intelligence Engine."""
    pass

@intelligence_cli.command("get")
@click.argument("insight_type", type=click.Choice(['community-detection', 'centrality'], case_sensitive=False))
def get_insight(insight_type: str):
    """
    Retrieves a specific graph insight from the intelligence engine.
    """
    click.echo(f"Requesting '{insight_type}' insight...")
    
    url = f"{INTELLIGENCE_SERVICE_URL}/{insight_type}"
    
    try:
        with httpx.Client() as client:
            # This endpoint can take a while, so we use a longer timeout.
            response = client.get(url, timeout=60.0)
            handle_api_error(response)
            click.echo("Insight received successfully:")
            click.echo(json.dumps(response.json(), indent=2))
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError:
        pass 