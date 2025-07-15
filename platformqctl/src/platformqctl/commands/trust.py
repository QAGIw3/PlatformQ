import click
import httpx
import json

# Configuration would come from a config file
VC_SERVICE_URL = "http://localhost:8005/api/v1/issue" 

def handle_api_error(response: httpx.Response):
    if response.status_code >= 400:
        click.echo(f"Error: API returned status {response.status_code} - {response.text}")
    response.raise_for_status()

@click.group("trust")
def trust_cli():
    """Commands for the Trust Engine (Verifiable Credentials)."""
    pass

@trust_cli.command("issue")
@click.option("--type", "credential_type", required=True, help="The type of the credential (e.g., EmployeeID).")
@click.option("--subject", required=True, help="The credential subject as a JSON string.")
@click.pass_context
def issue_credential(ctx, credential_type: str, subject: str):
    """
    Issues a new Verifiable Credential.
    
    Example: platformqctl trust issue --type EmployeeID --subject '{"id": "did:example:123", "name": "Alice"}'
    """
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: Config not found. Please run 'platformqctl init'.", err=True)
        return
    base_url = f"{config.get('api_gateway_url', '')}/verifiable-credential-service/api/v1/issue"

    click.echo(f"Issuing '{credential_type}' credential...")
    
    try:
        subject_json = json.loads(subject)
    except json.JSONDecodeError:
        click.echo("Error: --subject must be a valid JSON string.", err=True)
        return

    request_body = {
        "type": credential_type,
        "subject": subject_json,
    }

    try:
        with httpx.Client() as client:
            response = client.post(base_url, json=request_body)
            handle_api_error(response)
            click.echo("Credential issued successfully:")
            click.echo(json.dumps(response.json(), indent=2))
    except httpx.RequestError as exc:
        click.echo(f"An error occurred while requesting {exc.request.url!r}.")
    except httpx.HTTPStatusError:
        pass 