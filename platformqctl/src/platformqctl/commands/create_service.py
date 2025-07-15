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
    
    # 3. Automatically add the new service to the platformq-stack chart
    try:
        update_platform_stack(service_name)
        click.echo(f"Service '{service_name}' added as a dependency to the platformq-stack Helm chart.")
    except Exception as e:
        click.echo(f"Warning: Could not automatically update platformq-stack Helm chart: {e}", err=True)
        click.echo("Please add it to the platformq-stack Helm chart and CI/CD workflows manually.")
        return

    click.echo(f"Service '{service_name}' created and registered successfully.")

def update_platform_stack(service_name):
    """
    Adds the new service as a dependency to the platformq-stack Helm chart.
    """
    stack_chart_path = os.path.join(SERVICES_DIR, "..", "iac", "kubernetes", "charts", "platformq-stack", "Chart.yaml")
    stack_values_path = os.path.join(SERVICES_DIR, "..", "iac", "kubernetes", "charts", "platformq-stack", "values.yaml")
    
    # Read the template chart to get its version
    template_chart_path = os.path.join(TEMPLATE_DIR, "helm", "Chart.yaml")
    with open(template_chart_path, 'r') as f:
        template_chart = yaml.load(f)
    
    # 1. Update platformq-stack/Chart.yaml
    with open(stack_chart_path, 'r') as f:
        stack_chart = yaml.load(f)

    if 'dependencies' not in stack_chart:
        stack_chart['dependencies'] = []
    
    # Avoid adding if it already exists
    if not any(d['name'] == service_name for d in stack_chart['dependencies']):
        stack_chart['dependencies'].append({
            'name': service_name,
            'version': template_chart['version'],
            'repository': f'file://../../../services/{service_name}/helm'
        })

        with open(stack_chart_path, 'w') as f:
            yaml.dump(stack_chart, f)

    # 2. Update platformq-stack/values.yaml
    with open(stack_values_path, 'r') as f:
        stack_values = yaml.load(f)

    if service_name not in stack_values:
        # Add a default enabled: false configuration for the new service
        stack_values[service_name] = {'enabled': False}
        with open(stack_values_path, 'w') as f:
            yaml.dump(stack_values, f)
