"""
Market ML Training DAG

Orchestrates end-to-end machine learning training pipeline for market prediction models:
1. Data extraction from trading data lake
2. Feature engineering
3. Model training with multiple algorithms
4. Model evaluation and comparison
5. Model deployment to serving infrastructure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
import json

default_args = {
    'owner': 'ml-platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'market_ml_training_pipeline',
    default_args=default_args,
    description='ML training pipeline for market prediction models',
    schedule_interval='@daily',
    catchup=False,
    tags=['ml', 'trading', 'data-platform']
)


@task(dag=dag)
def extract_training_data(**context):
    """Extract training data from data platform"""
    import httpx
    import pandas as pd
    
    # Configuration
    data_platform_url = "http://data-platform-service:8000"
    
    # Extract features from trading data lake
    query = """
    SELECT 
        market_id,
        timestamp,
        price,
        volume,
        order_imbalance,
        spread,
        volatility,
        trader_skill_avg,
        market_sentiment,
        technical_indicators,
        LEAD(price, 1) OVER (PARTITION BY market_id ORDER BY timestamp) as target_price
    FROM ml_features.trading_features
    WHERE date >= CURRENT_DATE - INTERVAL '90' DAY
    AND market_id IN ('BTC-USD', 'ETH-USD', 'COMP-USD')
    """
    
    with httpx.Client() as client:
        response = client.post(
            f"{data_platform_url}/api/v1/query/execute",
            json={"sql": query}
        )
        response.raise_for_status()
        
    # Save to XCom for next tasks
    return {
        "query_id": response.json()["query_id"],
        "row_count": response.json()["row_count"],
        "features": response.json()["columns"]
    }


@task(dag=dag)
def prepare_ml_datasets(training_data):
    """Prepare datasets for ML training"""
    import httpx
    
    ml_platform_url = "http://unified-ml-platform-service:8000"
    
    # Create ML dataset
    with httpx.Client() as client:
        response = client.post(
            f"{ml_platform_url}/api/v1/datasets/prepare",
            json={
                "name": f"market_prediction_{datetime.now().strftime('%Y%m%d')}",
                "source_query_id": training_data["query_id"],
                "feature_engineering": {
                    "lag_features": [1, 5, 10, 30],
                    "rolling_windows": [5, 15, 30],
                    "technical_indicators": True,
                    "interaction_features": True
                },
                "split_config": {
                    "train_ratio": 0.7,
                    "val_ratio": 0.15,
                    "test_ratio": 0.15,
                    "time_based": True
                },
                "target_column": "target_price"
            }
        )
        response.raise_for_status()
        
    return response.json()["dataset_id"]


with TaskGroup("model_training", dag=dag) as model_training:
    
    @task
    def train_lstm_model(dataset_id):
        """Train LSTM model for time series prediction"""
        import httpx
        
        ml_platform_url = "http://unified-ml-platform-service:8000"
        
        with httpx.Client(timeout=3600) as client:
            response = client.post(
                f"{ml_platform_url}/api/v1/training/submit",
                json={
                    "dataset_id": dataset_id,
                    "algorithm": "lstm",
                    "framework": "tensorflow",
                    "hyperparameters": {
                        "lstm_units": [128, 64],
                        "dropout": 0.2,
                        "learning_rate": 0.001,
                        "batch_size": 64,
                        "epochs": 100,
                        "early_stopping_patience": 10
                    },
                    "compute_requirements": {
                        "gpu": True,
                        "gpu_type": "V100",
                        "memory": "32Gi"
                    }
                }
            )
            response.raise_for_status()
            
        return response.json()["training_id"]
    
    @task
    def train_xgboost_model(dataset_id):
        """Train XGBoost model"""
        import httpx
        
        ml_platform_url = "http://unified-ml-platform-service:8000"
        
        with httpx.Client(timeout=3600) as client:
            response = client.post(
                f"{ml_platform_url}/api/v1/training/submit",
                json={
                    "dataset_id": dataset_id,
                    "algorithm": "xgboost",
                    "framework": "xgboost",
                    "hyperparameters": {
                        "n_estimators": 1000,
                        "max_depth": 10,
                        "learning_rate": 0.01,
                        "subsample": 0.8,
                        "colsample_bytree": 0.8,
                        "objective": "reg:squarederror",
                        "early_stopping_rounds": 50
                    },
                    "compute_requirements": {
                        "cpu": 16,
                        "memory": "64Gi"
                    }
                }
            )
            response.raise_for_status()
            
        return response.json()["training_id"]
    
    @task
    def train_transformer_model(dataset_id):
        """Train Transformer model for market prediction"""
        import httpx
        
        ml_platform_url = "http://unified-ml-platform-service:8000"
        
        with httpx.Client(timeout=7200) as client:
            response = client.post(
                f"{ml_platform_url}/api/v1/training/submit",
                json={
                    "dataset_id": dataset_id,
                    "algorithm": "temporal_fusion_transformer",
                    "framework": "pytorch",
                    "hyperparameters": {
                        "hidden_size": 256,
                        "lstm_layers": 2,
                        "num_attention_heads": 8,
                        "dropout": 0.1,
                        "learning_rate": 0.001,
                        "batch_size": 128,
                        "epochs": 50,
                        "gradient_clip_val": 0.1
                    },
                    "compute_requirements": {
                        "gpu": True,
                        "gpu_type": "A100",
                        "gpu_count": 2,
                        "memory": "64Gi"
                    }
                }
            )
            response.raise_for_status()
            
        return response.json()["training_id"]
    
    @task
    def train_ensemble_model(dataset_id):
        """Train ensemble model combining multiple algorithms"""
        import httpx
        
        ml_platform_url = "http://unified-ml-platform-service:8000"
        
        with httpx.Client(timeout=3600) as client:
            response = client.post(
                f"{ml_platform_url}/api/v1/training/submit",
                json={
                    "dataset_id": dataset_id,
                    "algorithm": "ensemble",
                    "framework": "sklearn",
                    "base_models": [
                        {"algorithm": "random_forest", "weight": 0.3},
                        {"algorithm": "gradient_boosting", "weight": 0.3},
                        {"algorithm": "neural_network", "weight": 0.4}
                    ],
                    "ensemble_method": "weighted_average",
                    "compute_requirements": {
                        "cpu": 32,
                        "memory": "128Gi"
                    }
                }
            )
            response.raise_for_status()
            
        return response.json()["training_id"]


@task(dag=dag)
def evaluate_models(training_ids):
    """Evaluate and compare trained models"""
    import httpx
    import pandas as pd
    
    ml_platform_url = "http://unified-ml-platform-service:8000"
    
    # Collect evaluation metrics
    evaluations = []
    
    with httpx.Client() as client:
        for model_name, training_id in training_ids.items():
            response = client.get(
                f"{ml_platform_url}/api/v1/training/{training_id}/evaluation"
            )
            response.raise_for_status()
            
            eval_data = response.json()
            evaluations.append({
                "model": model_name,
                "training_id": training_id,
                "mse": eval_data["metrics"]["mse"],
                "mae": eval_data["metrics"]["mae"],
                "r2": eval_data["metrics"]["r2"],
                "directional_accuracy": eval_data["metrics"].get("directional_accuracy", 0),
                "sharpe_ratio": eval_data["metrics"].get("sharpe_ratio", 0)
            })
    
    # Select best model
    eval_df = pd.DataFrame(evaluations)
    
    # Multi-criteria selection (minimize MSE, maximize sharpe ratio)
    eval_df["score"] = -eval_df["mse"] + 2 * eval_df["sharpe_ratio"]
    best_model = eval_df.loc[eval_df["score"].idxmax()]
    
    return {
        "best_model": best_model["model"],
        "best_training_id": best_model["training_id"],
        "evaluations": evaluations
    }


@task(dag=dag)
def deploy_best_model(evaluation_results):
    """Deploy best model to serving infrastructure"""
    import httpx
    
    ml_platform_url = "http://unified-ml-platform-service:8000"
    
    best_training_id = evaluation_results["best_training_id"]
    
    # Register model for serving
    with httpx.Client() as client:
        response = client.post(
            f"{ml_platform_url}/api/v1/models/register",
            json={
                "training_id": best_training_id,
                "model_name": "market_predictor",
                "version": f"v{datetime.now().strftime('%Y%m%d')}",
                "tags": ["production", "market_prediction", "auto_deployed"],
                "serving_config": {
                    "instances": 3,
                    "cpu": 4,
                    "memory": "16Gi",
                    "gpu": evaluation_results["best_model"] in ["lstm", "transformer"]
                }
            }
        )
        response.raise_for_status()
        
        model_id = response.json()["model_id"]
        
        # Deploy to production
        response = client.post(
            f"{ml_platform_url}/api/v1/models/{model_id}/deploy",
            json={
                "environment": "production",
                "traffic_percentage": 100,
                "monitoring_enabled": True,
                "alert_thresholds": {
                    "prediction_latency_p99": 100,  # ms
                    "error_rate": 0.01
                }
            }
        )
        response.raise_for_status()
        
    return response.json()


@task(dag=dag)
def update_market_intelligence_service(deployment_info):
    """Update market intelligence service with new model"""
    import httpx
    
    market_intel_url = "http://market-intelligence-service:8000"
    
    with httpx.Client() as client:
        response = client.post(
            f"{market_intel_url}/api/v1/models/update",
            json={
                "model_endpoint": deployment_info["endpoint_url"],
                "model_version": deployment_info["version"],
                "model_type": "price_prediction",
                "features": deployment_info["feature_list"]
            }
        )
        response.raise_for_status()
        
    return "Model updated in market intelligence service"


@task(dag=dag)
def create_monitoring_dashboard(deployment_info):
    """Create monitoring dashboard for the deployed model"""
    import httpx
    
    analytics_url = "http://analytics-service:8000"
    
    dashboard_config = {
        "name": f"Market Prediction Model - {deployment_info['version']}",
        "panels": [
            {
                "title": "Prediction Accuracy",
                "type": "timeseries",
                "queries": [
                    {
                        "datasource": "druid",
                        "query": f"""
                        SELECT 
                            TIME_FLOOR(__time, 'PT1H') as time,
                            AVG(ABS(predicted_price - actual_price) / actual_price) as mape,
                            COUNT(*) as prediction_count
                        FROM model_predictions
                        WHERE model_id = '{deployment_info['model_id']}'
                        GROUP BY 1
                        """
                    }
                ]
            },
            {
                "title": "Directional Accuracy",
                "type": "gauge",
                "query": {
                    "datasource": "ignite",
                    "cache": "model_metrics",
                    "metric": "directional_accuracy"
                }
            },
            {
                "title": "Prediction Latency",
                "type": "histogram",
                "query": {
                    "datasource": "prometheus",
                    "metric": "model_prediction_latency_seconds",
                    "labels": {"model_id": deployment_info["model_id"]}
                }
            }
        ],
        "refresh_interval": "30s",
        "time_range": "24h"
    }
    
    with httpx.Client() as client:
        response = client.post(
            f"{analytics_url}/api/v1/dashboards/create",
            json=dashboard_config
        )
        response.raise_for_status()
        
    return response.json()["dashboard_url"]


# Task dependencies
training_data = extract_training_data()
dataset_id = prepare_ml_datasets(training_data)

# Train models in parallel
with model_training:
    lstm_id = train_lstm_model(dataset_id)
    xgboost_id = train_xgboost_model(dataset_id)
    transformer_id = train_transformer_model(dataset_id)
    ensemble_id = train_ensemble_model(dataset_id)

# Collect training IDs
training_ids = {
    "lstm": lstm_id,
    "xgboost": xgboost_id,
    "transformer": transformer_id,
    "ensemble": ensemble_id
}

# Evaluate and deploy
evaluation = evaluate_models(training_ids)
deployment = deploy_best_model(evaluation)
update_intel = update_market_intelligence_service(deployment)
dashboard = create_monitoring_dashboard(deployment)

# Set task dependencies
dataset_id >> model_training
[lstm_id, xgboost_id, transformer_id, ensemble_id] >> evaluation
evaluation >> deployment >> [update_intel, dashboard] 