from openproject import OpenProject

def create_openproject_project(openproject_client: OpenProject, tenant_id: str, tenant_name: str):
    """
    Creates a new project for a tenant.
    """
    project_name = f"Tenant {tenant_name} Project"
    project_identifier = f"tenant-{tenant_id}"
    
    try:
        openproject_client.projects.create(
            name=project_name,
            identifier=project_identifier
        )
    except Exception as e:
        # This may fail if the project already exists, which is fine.
        # In a real app, you would want more robust error handling.
        print(f"Could not create project {project_name}: {e}") 