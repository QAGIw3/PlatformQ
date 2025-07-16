# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the requirements file into the container at /usr/src/app
COPY requirements.in .

# Install any needed packages specified in requirements.in
RUN pip install --no-cache-dir -r requirements.in

# Copy the rest of the application's code into the container
COPY ./app /usr/src/app/app

# Make port 80 available to the world outside this container
# (Not strictly necessary for this service, but good practice)
EXPOSE 80

# Define environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /usr/src/app

# Run main.py when the container launches
CMD ["python", "app/main.py"]
