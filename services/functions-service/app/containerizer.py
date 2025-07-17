import docker
import tempfile
import shutil

def build_and_push_image(name: str, code: str, requirements: str) -> str:
    """
    Builds a Docker image with the given code and requirements, and pushes it to a registry.
    """
    client = docker.from_env()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a Dockerfile
        dockerfile_content = f"""
        FROM python:3.9-slim
        WORKDIR /app
        COPY requirements.txt .
        RUN pip install --no-cache-dir -r requirements.txt
        COPY . .
        CMD ["python", "main.py"]
        """
        with open(f"{tmpdir}/Dockerfile", "w") as f:
            f.write(dockerfile_content)
            
        # Create the main.py file
        with open(f"{tmpdir}/main.py", "w") as f:
            f.write(code)
            
        # Create the requirements.txt file
        with open(f"{tmpdir}/requirements.txt", "w") as f:
            f.write(requirements)
            
        # Build the image
        image_tag = f"platformq/{name}:latest"
        image, _ = client.images.build(path=tmpdir, tag=image_tag, rm=True)
        
        # In a real app, you would push the image to a container registry.
        # For now, we'll just return the image tag.
        print(f"Built image {image_tag}")
        
        return image_tag 