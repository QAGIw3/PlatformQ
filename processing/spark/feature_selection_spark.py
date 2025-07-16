"""
PySpark script for Quantum-Assisted Feature Selection.

This script performs two main functions:
1.  Uses a classical feature selection method (Chi-Squared) to select the most
    promising features from a larger dataset.
2.  Constructs a QUBO (Quadratic Unconstrained Binary Optimization) problem
    from the selected features' covariance matrix to find the optimal,
    least-redundant subset.

The output of this script is a JSON file containing the QUBO matrix, which can
be passed to the quantum-optimization-service.
"""
import argparse
import json
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, ChiSqSelector
from pyspark.ml.stat import Correlation
import numpy as np

def create_spark_session(app_name="QuantumFeatureSelection"):
    """Creates and returns a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def generate_qubo_from_covariance(covariance_matrix: np.ndarray, alpha: float = 0.5) -> np.ndarray:
    """
    Generates a QUBO matrix from a covariance matrix.

    The diagonal elements of the QUBO represent the variance of each feature (its
    individual importance), and the off-diagonal elements represent the covariance
    (the interaction or redundancy between features).

    Args:
        covariance_matrix: A numpy array representing the covariance of the features.
        alpha: A regularization parameter to balance feature importance vs. redundancy.
               alpha=1 focuses only on removing redundancy.
               alpha=0 focuses only on individual importance (variance).

    Returns:
        A numpy array representing the QUBO matrix.
    """
    # The diagonal terms should be minimized if variance is high, so we use negative variance.
    # We want to penalize high covariance (redundancy), so off-diagonal terms are positive.
    # The 'alpha' term balances the two objectives.
    qubo_matrix = (alpha * covariance_matrix) - ((1 - alpha) * np.diag(np.diag(covariance_matrix)))
    return qubo_matrix

def main(input_path: str, output_path: str, num_top_features: int, label_col: str):
    """
    Main execution function.

    Args:
        input_path: Path to the input dataset (e.g., Parquet, CSV).
        output_path: Path to write the output QUBO JSON file.
        num_top_features: The number of top features to select with ChiSqSelector.
        label_col: The name of the label column in the dataset.
    """
    spark = create_spark_session()
    
    # Load data
    # Assuming CSV format for this example. For production, Parquet is preferred.
    df = spark.read.option("inferSchema", "true").option("header", "true").csv(input_path)
    
    feature_cols = [col for col in df.columns if col != label_col]
    
    # 1. Assemble features into a single vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vector = assembler.transform(df).select("features", label_col)
    
    # 2. Use ChiSqSelector for initial feature selection
    selector = ChiSqSelector(numTopFeatures=num_top_features, featuresCol="features",
                             outputCol="selectedFeatures", labelCol=label_col)
    selector_model = selector.fit(df_vector)
    
    # Get the names of the selected features
    selected_indices = selector_model.selectedFeatures
    selected_feature_names = [feature_cols[i] for i in selected_indices]
    
    df_selected = selector_model.transform(df_vector)
    
    print(f"Selected {len(selected_feature_names)} features using ChiSqSelector: {selected_feature_names}")
    
    # 3. Calculate the covariance matrix for the selected features
    # Note: Spark's Correlation uses Pearson correlation to compute the covariance matrix
    corr_matrix = Correlation.corr(df_selected, "selectedFeatures", "pearson").head()
    covariance_matrix = corr_matrix[0].toArray()
    
    print("Calculated Covariance Matrix:")
    print(covariance_matrix)
    
    # 4. Generate the QUBO matrix
    # We use a simple formulation here. More advanced formulations might consider
    # feature correlation with the target label as well.
    qubo_matrix = generate_qubo_from_covariance(covariance_matrix, alpha=0.75)
    
    print("Generated QUBO Matrix:")
    print(qubo_matrix)
    
    # 5. Save the QUBO problem to a JSON file
    qubo_problem = {
        "problem_type": "qubo",
        "problem_data": {
            "qubo_matrix": qubo_matrix.tolist()
        },
        "metadata": {
            "num_variables": len(selected_feature_names),
            "variable_names": selected_feature_names,
            "source": "Spark ChiSqSelector + Covariance"
        }
    }
    
    with open(output_path, 'w') as f:
        json.dump(qubo_problem, f, indent=4)
        
    print(f"Successfully wrote QUBO problem to {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Quantum-Assisted Feature Selection")
    parser.add_argument("--input", required=True, help="Input dataset path")
    parser.add_argument("--output", required=True, help="Output QUBO JSON path")
    parser.add_argument("--label", required=True, help="Name of the label column")
    parser.add_argument("--num_features", type=int, default=20, help="Number of top features to select")
    
    args = parser.parse_args()
    
    main(args.input, args.output, args.num_features, args.label) 