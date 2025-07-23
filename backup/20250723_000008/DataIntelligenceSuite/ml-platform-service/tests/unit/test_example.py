"""Tests for Unified ML Platform Service"""

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

def test_health_check():
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_example_get():
    """Test example GET endpoint"""
    response = client.get("/api/v1/example/")
    assert response.status_code == 200
    assert "message" in response.json()

def test_example_post():
    """Test example POST endpoint"""
    data = {"test": "data"}
    response = client.post("/api/v1/example/", json=data)
    assert response.status_code == 200
    assert response.json()["data"] == data
