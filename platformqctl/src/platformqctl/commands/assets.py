import click
import httpx
import uuid

# Configuration - In a real app, this would come from a config file.
BASE_URL = "http://localhost:8000/api/v1/assets" 

def handle_api_error(response: httpx.Response):
    """Helper function to handle common API errors."""
    if response.status_code == 404:
        click.echo(f"Error: Resource not found. (404)")
    elif response.status_code >= 400:
        click.echo(f"Error: API returned status {response.status_code}")
        try:
            click.echo(f"Detail: {response.json()['detail']}")
        except (ValueError, KeyError):
            pass
    response.raise_for_status()

@click.group("assets")
def assets_cli():
    """Commands for interacting with Digital Assets."""
    pass

@assets_cli.command("get")
@click.argument("asset_id", type=click.UUID)
def get_asset(asset_id: uuid.UUID):
    """Retrieves metadata for a specific Digital Asset."""
    click.echo(f"Fetching asset {asset_id}...")
    try:
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/{asset_id}")
            handle_api_error(response)
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")


@assets_cli.command("list")
@click.option("--asset-type", help="Filter by asset type (e.g., CRM_CONTACT).")
@click.option("--limit", default=10, help="Number of assets to return.")
def list_assets(asset_type: str, limit: int):
    """Lists Digital Assets on the platform."""
    click.echo("Listing assets...")
    params = {"limit": limit}
    if asset_type:
        params["asset_type"] = asset_type
    
    try:
        with httpx.Client() as client:
            response = client.get(BASE_URL, params=params)
            handle_api_error(response)
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")

@assets_cli.command("create")
@click.option("--name", required=True, help="Name for the new asset.")
@click.option("--type", "asset_type", required=True, help="Type of the asset (e.g., 3D_MODEL).")
@click.option("--owner-id", required=True, type=click.UUID, help="UUID of the owner.")
@click.option("--tag", "tags", multiple=True, help="Add one or more tags.")
def create_asset(name: str, asset_type: str, owner_id: uuid.UUID, tags: list):
    """Creates a new Digital Asset."""
    click.echo(f"Creating asset '{name}'...")
    
    asset_data = {
        "asset_name": name,
        "asset_type": asset_type,
        "owner_id": str(owner_id),
        "tags": list(tags)
    }

    try:
        with httpx.Client() as client:
            response = client.post(BASE_URL, json=asset_data)
            handle_api_error(response)
            click.echo("Asset created successfully:")
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")
