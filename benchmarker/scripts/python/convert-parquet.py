import h5py
import numpy as np
import pandas as pd
import json
import argparse
from pathlib import Path
from typing import Optional
from datasets import Dataset, Features, Value, Array2D, Sequence


def convert_hdf5_to_parquet(hdf5_file: str, output_dir: str, dataset_name: str, distance: str) -> None:
    """
    Convert an ANN benchmark HDF5 file to the my-ann-benchmarks format.
    
    Args:
        hdf5_file: Path to the input HDF5 file
        output_dir: Directory to save the parquet files
        dataset_name: Name for the dataset (used for directory naming)
        distance: Distance metric ("angular", "dot", or "euclidean")
    """
    output_path = Path(output_dir)
    dataset_path = output_path / dataset_name
    dataset_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Converting {hdf5_file} to my-ann-benchmarks format...")
    
    with h5py.File(hdf5_file, "r") as hf:
        # Print available datasets
        print(f"Available datasets: {list(hf.keys())}")
        
        # Convert train data
        if "train" in hf:
            train_data = hf["train"][:]
            print(f"Train dimensions: {train_data.shape}")
            
            # Convert embeddings to variable-length byte arrays
            embedding_bytes = []
            for row in train_data:
                # Store all embeddings as float32
                float32_array = row.astype(np.float32)
                embedding_bytes.append(float32_array.tobytes())

            print(f"Length of embeddings {len(embedding_bytes)}")
            
            # Convert to DataFrame with embedding and id columns
            train_df = pd.DataFrame({
                "id": range(len(train_data)),
                "embedding": embedding_bytes
            })
            
            # Define schema with variable-length byte array for embeddings
            train_features = Features({
                "id": Value("int64"),
                "embedding": Value("binary")  # Variable-length binary data (BYTE_ARRAY)
            })
            
            # Create train dataset with explicit schema and save as parquet
            train_dataset = Dataset.from_pandas(train_df, features=train_features)
            train_path = dataset_path / "train"
            train_path.mkdir(exist_ok=True)
            train_dataset.to_parquet(str(train_path / "train.parquet"))
            print(f"Saved train data to {train_path}")
        
        # Convert test data
        if "test" in hf:
            test_data = hf["test"][:]
            print(f"Test dimensions: {test_data.shape}")
            
            # Convert embeddings to variable-length byte arrays
            test_embedding_bytes = []
            for row in test_data:
                # Store all embeddings as float32
                float32_array = row.astype(np.float32)
                test_embedding_bytes.append(float32_array.tobytes())
            
            # Convert to DataFrame with embedding and id columns
            test_df = pd.DataFrame({
                "id": range(len(test_data)),
                "embedding": test_embedding_bytes
            })
            
            # Add neighbors data if available
            if "neighbors" in hf:
                neighbors_data = hf["neighbors"][:]
                print(f"Neighbors dimensions: {neighbors_data.shape}")
                test_df["neighbors"] = neighbors_data.tolist()
            
            # Define schema with variable-length byte array for embeddings
            test_features = Features({
                "id": Value("int64"),
                "embedding": Value("binary")  # Variable-length binary data (BYTE_ARRAY)
            })
            
            # Add neighbors to schema if available
            if "neighbors" in hf:
                # Use Sequence feature for variable-length lists of integers
                test_features["neighbors"] = Sequence(Value("int64"))
            
            # Create test dataset with explicit schema and save as parquet
            test_dataset = Dataset.from_pandas(test_df, features=test_features)
            test_path = dataset_path / "test"
            test_path.mkdir(exist_ok=True)
            test_dataset.to_parquet(str(test_path / "test.parquet"))
            print(f"Saved test data to {test_path}")
        
        # Create dataset info for this subset
        subset_info = {
            "description": f"ANN benchmark dataset: {dataset_name}",
            "dataset_name": dataset_name,
            "distance": distance,
            "dimensions": train_data.shape[1] if "train" in hf else None,
            "embedding_format": "variable_length_byte_array",
            "embedding_dtype": "float32",
            "embedding_byte_order": "little_endian",
            "embedding_size_bytes": len(embedding_bytes[0]) if "train" in hf and embedding_bytes else None,
            "splits": {
                "train": len(train_data) if "train" in hf else 0,
                "test": len(test_data) if "test" in hf else 0
            }
        }
        
        # Save dataset info in the subset directory
        info_file = dataset_path / "dataset_info.json"
        with open(info_file, "w") as f:
            json.dump(subset_info, f, indent=2)
        print(f"Saved dataset info to {info_file}")
    
    print(f"Successfully converted {hdf5_file} to my-ann-benchmarks format in {output_dir}")


def upload_to_hub(dataset_dir: str, repo_id: str, dataset_name: str, token: Optional[str] = None) -> None:
    """
    Upload a specific converted dataset to Hugging Face Hub.
    
    Args:
        dataset_dir: Directory containing the converted datasets
        repo_id: Repository ID for the Hugging Face Hub (e.g., "username/dataset_name")
        dataset_name: Name of the specific dataset to upload
        token: Hugging Face token (optional, will use cached token if not provided)
    """
    from huggingface_hub import HfApi
    from pathlib import Path
    
    dataset_path = Path(dataset_dir)
    subset_dir = dataset_path / dataset_name
    api = HfApi(token=token)
    
    if not subset_dir.exists():
        print(f"Error: Dataset directory {subset_dir} does not exist")
        return
    
    print(f"Uploading dataset '{dataset_name}' to {repo_id}...")
    
    # Upload dataset_info.json
    info_file = subset_dir / "dataset_info.json"
    if info_file.exists():
        print(f"Uploading dataset info for {dataset_name}...")
        api.upload_file(
            path_or_fileobj=str(info_file),
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo=f"{dataset_name}/dataset_info.json"
        )
    
    # Upload train folder
    train_folder = subset_dir / "train"
    if train_folder.exists():
        print(f"Uploading train folder for {dataset_name}...")
        api.upload_folder(
            folder_path=str(train_folder),
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo=f"{dataset_name}/train",
            allow_patterns="*.parquet"
        )
    
    # Upload test folder
    test_folder = subset_dir / "test"
    if test_folder.exists():
        print(f"Uploading test folder for {dataset_name}...")
        api.upload_folder(
            folder_path=str(test_folder),
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo=f"{dataset_name}/test",
            allow_patterns="*.parquet"
        )
    
    print(f"Successfully uploaded dataset '{dataset_name}' to https://huggingface.co/datasets/{repo_id}")


def main():
    parser = argparse.ArgumentParser(description="Convert ANN benchmark HDF5 file to Parquet format")
    parser.add_argument("--input", required=True, help="Path to the input HDF5 file")
    parser.add_argument("--output-dir", required=True, help="Directory to save the parquet files")
    parser.add_argument("--name", required=True, help="Name for the dataset subset")
    parser.add_argument("--distance", required=True, choices=["angular", "dot", "euclidean"], help="Distance metric for the dataset")
    parser.add_argument("--upload", action="store_true", help="Upload to Hugging Face Hub after conversion")
    parser.add_argument("--repo-id", help="Repository ID for Hugging Face Hub (e.g., username/dataset_name)")
    parser.add_argument("--token", help="Hugging Face token (optional)")
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.input).exists():
        print(f"Error: Input file {args.input} does not exist")
        return 1
    
    try:
        # Convert the file
        convert_hdf5_to_parquet(args.input, args.output_dir, args.name, args.distance)
        
        # Upload if requested
        if args.upload:
            if not args.repo_id:
                print("Error: --repo-id is required when using --upload")
                return 1
            upload_to_hub(args.output_dir, args.repo_id, args.name, args.token)
        
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
