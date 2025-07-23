# --- Builder Stage ---
# This stage installs all dependencies, including dev dependencies,
# and can be used to run tests and linters in CI.
FROM python:3.11-slim as builder

WORKDIR /app

# Install all dependencies
COPY ./requirements.txt /app/
COPY ../shared /app/shared
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY ./app /app/app
COPY ./scripts /app/scripts
COPY ./tests /app/tests


# --- Final Stage ---
# This is the production image. It's smaller and more secure.
FROM python:3.11-slim as final

WORKDIR /app

# Create a non-root user for security
RUN useradd --create-home appuser
USER appuser

# Copy installed production dependencies from the builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy only the necessary application code
COPY --from=builder /app/app /app/app
COPY --from=builder /app/shared /app/shared

# Make port 80 available
EXPOSE 80

# Run uvicorn server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"] 