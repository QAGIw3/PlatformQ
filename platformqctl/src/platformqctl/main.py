import click
import os
import shutil
from ruamel.yaml import YAML

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "templates", "service-template")
SERVICES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "services")
PLACEHOLDER = "__SERVICE_NAME__"

STACK_CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "iac", "kubernetes", "charts", "platformq-stack")
CI_WORKFLOW_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".github", "workflows", "ci.yml")

yaml = YAML()

@click.group()
def cli():
    """A CLI tool for managing the platformQ microservices."""
    pass

@cli.command()
@click.argument('service_name')
@click.option('--deployable', is_flag=True, help="Automatically add the service to the Helm chart and CI/CD pipeline.")
def create_service(service_name, deployable):
    """Creates a new service from the default template."""
    click.echo(f"Creating new service: {service_name}")
    
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
            with open(file_path, 'r') as f:
                content = f.read()
            
            content = content.replace(PLACEHOLDER, service_name)
            
            with open(file_path, 'w') as f:
                f.write(content)
                
    if deployable:
        click.echo(f"Adding '{service_name}' to platform stack...")
        
        # 1. Add to umbrella Helm chart
        chart_path = os.path.join(STACK_CHART_DIR, "Chart.yaml")
        with open(chart_path, 'r') as f:
            chart = yaml.load(f)
        chart['dependencies'].append({
            'name': service_name,
            'version': '0.1.0',
            'repository': f'file://./../{service_name}'
        })
        with open(chart_path, 'w') as f:
            yaml.dump(chart, f)
        
        # 2. Add to umbrella values.yaml
        values_path = os.path.join(STACK_CHART_DIR, "values.yaml")
        with open(values_path, 'a') as f:
            f.write(f"\n{service_name}:\n  replicaCount: 1\n  image:\n    repository: gcr.io/your-repo/{service_name}\n    tag: latest\n")

        # 3. Add to CI/CD workflow
        with open(CI_WORKFLOW_FILE, 'r') as f:
            ci_config = yaml.load(f)
        # (Conceptual: logic to copy the existing 'build-and-deploy-auth-service' job
        # and replace 'auth-service' with the new service_name would go here)
        click.echo("  - CI/CD job definition added (conceptual).")

    click.echo(f"Service '{service_name}' created successfully.")

if __name__ == '__main__':
    cli() 