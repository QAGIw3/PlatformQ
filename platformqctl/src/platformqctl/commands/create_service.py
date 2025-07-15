import click
import os
import shutil
from ruamel.yaml import YAML

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "templates", "service-template")
SERVICES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "services")
PLACEHOLDER = "__SERVICE_NAME__"
yaml = YAML()

@click.group("service")
def service_cli():
    """Commands for creating and managing services."""
    pass

@service_cli.command("create")
@click.argument('service_name')
def create_service(service_name):
    """Creates a new service from the default template."""
    target_dir = os.path.join(SERVICES_DIR, service_name)
    if os.path.exists(target_dir):
        click.echo(f"Error: Service '{service_name}' already exists.")
        return

    # 1. Copy the template directory
    shutil.copytree(TEMPLATE_DIR, target_dir)

    # 2. Walk through the new directory and replace placeholders
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                content = content.replace(PLACEHOLDER, service_name)
                
                with open(file_path, 'w') as f:
                    f.write(content)
            except UnicodeDecodeError:
                # Ignore binary files that can't be read
                click.echo(f"Skipping binary file: {filename}")


    click.echo(f"Service '{service_name}' created successfully in '{target_dir}'.")
    click.echo("Remember to add it to the platformq-stack Helm chart and CI/CD workflows if it's a deployable service.")
