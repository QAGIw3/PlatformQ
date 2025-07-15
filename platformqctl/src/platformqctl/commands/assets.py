import click
import httpx
import uuid

# Re-using the helper from the assets command would be ideal in a real app
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
@click.pass_context
def get_asset(ctx, asset_id: uuid.UUID):
    """Retrieves metadata for a specific Digital Asset."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/assets-service/api/v1/assets"

    click.echo(f"Fetching asset {asset_id}...")
    try:
        with httpx.Client() as client:
            response = client.get(f"{base_url}/{asset_id}")
            handle_api_error(response)
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")


@assets_cli.command("list")
@click.option("--asset-type", help="Filter by asset type (e.g., CRM_CONTACT).")
@click.option("--limit", default=10, help="Number of assets to return.")
@click.pass_context
def list_assets(ctx, asset_type: str, limit: int):
    """Lists Digital Assets on the platform."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/assets-service/api/v1/assets"

    click.echo("Listing assets...")
    params = {"limit": limit}
    if asset_type:
        params["asset_type"] = asset_type
    
    try:
        with httpx.Client() as client:
            response = client.get(base_url, params=params)
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
@click.pass_context
def create_asset(ctx, name: str, asset_type: str, owner_id: uuid.UUID, tags: list):
    """Creates a new Digital Asset."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/assets-service/api/v1/assets"

    click.echo(f"Creating asset '{name}'...")
    
    asset_data = {
        "asset_name": name,
        "asset_type": asset_type,
        "owner_id": str(owner_id),
        "tags": list(tags)
    }

    try:
        with httpx.Client() as client:
            response = client.post(base_url, json=asset_data)
            handle_api_error(response)
            click.echo("Asset created successfully:")
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")

@assets_cli.command("update")
@click.argument("asset_id", type=click.UUID)
@click.option("--name", help="New name for the asset.")
@click.option("--tag", "tags", multiple=True, help="Set new tags for the asset.")
@click.pass_context
def update_asset(ctx, asset_id: uuid.UUID, name: str, tags: list):
    """Updates an existing Digital Asset."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/assets-service/api/v1/assets"
    
    update_data = {}
    if name:
        update_data['asset_name'] = name
    if tags:
        update_data['tags'] = list(tags)

    if not update_data:
        click.echo("Nothing to update. Please provide --name or --tag.", err=True)
        return

    click.echo(f"Updating asset {asset_id}...")
    try:
        with httpx.Client() as client:
            response = client.patch(f"{base_url}/{asset_id}", json=update_data)
            handle_api_error(response)
            click.echo("Asset updated successfully:")
            click.echo(response.text)
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")

@assets_cli.command("delete")
@click.argument("asset_id", type=click.UUID)
@click.pass_context
def delete_asset(ctx, asset_id: uuid.UUID):
    """Deletes a Digital Asset."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/assets-service/api/v1/assets"
    
    click.confirm(f"Are you sure you want to delete asset {asset_id}?", abort=True)
    
    click.echo(f"Deleting asset {asset_id}...")
    try:
        with httpx.Client() as client:
            response = client.delete(f"{base_url}/{asset_id}")
            handle_api_error(response)
            click.echo("Asset deleted successfully.")
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError as exc:
        click.echo(f"HTTP error for {exc.request.url!r}.")
