import h5py
import numpy as np
import faiss
import argparse
from pathlib import Path


def create_faiss_index(dimensions: int, distance: str):
    """
    Create a FAISS index based on the distance metric.
    
    Args:
        dimensions: Number of dimensions in the vectors
        distance: Distance metric ("angular", "dot", or "euclidean")
    
    Returns:
        FAISS index instance
    """
    if distance == "angular":
        # For angular distance, we normalize vectors and use inner product
        index = faiss.IndexFlatIP(dimensions)
    elif distance == "dot":
        # For dot product, use inner product
        index = faiss.IndexFlatIP(dimensions)
    elif distance == "euclidean":
        # For euclidean distance, use L2
        index = faiss.IndexFlatL2(dimensions)
    else:
        raise ValueError(f"Unsupported distance metric: {distance}. Must be 'angular', 'dot', or 'euclidean'")
    
    return index


def normalize_vectors(vectors: np.ndarray) -> np.ndarray:
    """Normalize vectors to unit length for angular distance."""
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1  # Avoid division by zero
    return vectors / norms


def reduce_dataset(
    input_file: str,
    output_file: str,
    train_size: int,
    distance: str,
    limit: int = 100,
    random_sample: bool = False,
    seed: int = None
) -> None:
    """
    Reduce the training dataset and recompute neighbors using FAISS brute force.
    
    Args:
        input_file: Path to the input HDF5 file
        output_file: Path to the output HDF5 file
        train_size: Target size for the training dataset
        distance: Distance metric ("angular", "dot", or "euclidean")
        limit: Number of neighbors to compute for each test query (default: 100)
        random_sample: If True, randomly sample training data; otherwise take first N
        seed: Random seed for sampling (optional)
    """
    if seed is not None:
        np.random.seed(seed)
    
    print(f"Reading input file: {input_file}")
    with h5py.File(input_file, "r") as hf:
        # Read original data
        original_train = hf["train"][:]
        original_test = hf["test"][:]
        
        print(f"Original train dimensions: {original_train.shape}")
        print(f"Original test dimensions: {original_test.shape}")
        
        # Determine how many training samples to keep
        original_train_size = original_train.shape[0]
        target_size = min(train_size, original_train_size)
        
        if target_size < original_train_size:
            print(f"Reducing training dataset from {original_train_size} to {target_size} samples")
            
            if random_sample:
                # Randomly sample indices
                indices = np.random.choice(original_train_size, size=target_size, replace=False)
                indices = np.sort(indices)  # Sort to maintain some order
                print(f"Randomly sampled {target_size} training vectors")
            else:
                # Take first N samples
                indices = np.arange(target_size)
                print(f"Taking first {target_size} training vectors")
            
            # Sample the training data
            reduced_train = original_train[indices]
        else:
            print(f"Training dataset size ({original_train_size}) is already <= target size ({train_size}), keeping all")
            reduced_train = original_train
            indices = np.arange(original_train_size)
        
        dimensions = reduced_train.shape[1]
        print(f"Train dimensions after reduction: {reduced_train.shape}")
        print(f"Building FAISS flat index for {dimensions} dimensions with {distance} distance...")
        
        # Prepare vectors based on distance metric
        if distance == "angular":
            # Normalize both train and test vectors for angular distance
            train_vectors = normalize_vectors(reduced_train.astype(np.float32))
            test_vectors = normalize_vectors(original_test.astype(np.float32))
        else:
            train_vectors = reduced_train.astype(np.float32)
            test_vectors = original_test.astype(np.float32)
        
        # Create FAISS index
        index = create_faiss_index(dimensions, distance)
        index.add(train_vectors)
        print(f"Index built with {index.ntotal} vectors")
        
        # Compute neighbors for all test queries
        print(f"Computing neighbors for {len(test_vectors)} test queries (limit={limit})...")
        D, I = index.search(test_vectors, limit)
        
        # Map indices back to original training set indices if we sampled
        if target_size < original_train_size:
            # I contains indices into the reduced training set
            # Map them back to original indices
            neighbors_data = indices[I].astype(np.int64)
        else:
            neighbors_data = I.astype(np.int64)
        
        print(f"Neighbors dimensions: {neighbors_data.shape}")
        print(f"Sample neighbors[0]: {neighbors_data[0][:5]}...")
        
        # Write output file
        print(f"Writing output file: {output_file}")
        with h5py.File(output_file, "w") as out_hf:
            # Write train dataset (use original dtype)
            out_hf.create_dataset("train", data=reduced_train)
            
            # Write test dataset (unchanged)
            out_hf.create_dataset("test", data=original_test)
            
            # Write neighbors dataset
            out_hf.create_dataset("neighbors", data=neighbors_data)
            
            # Copy any other datasets that might exist (like distance metadata, etc.)
            # but skip filter-related datasets as requested
            skip_datasets = {"train", "test", "neighbors", "train_categories", 
                           "test_categories", "train_properties", "test_properties", "filters"}
            
            for key in hf.keys():
                if key not in skip_datasets:
                    print(f"Copying dataset: {key}")
                    out_hf.create_dataset(key, data=hf[key][:])
        
        print(f"Successfully created reduced dataset: {output_file}")
        print(f"  Train size: {reduced_train.shape[0]} (was {original_train_size})")
        print(f"  Test size: {original_test.shape[0]} (unchanged)")
        print(f"  Neighbors: {neighbors_data.shape}")


def main():
    parser = argparse.ArgumentParser(
        description="Reduce training dataset size and recompute neighbors using FAISS brute force"
    )
    parser.add_argument("input_file", help="Path to the input HDF5 file")
    parser.add_argument("output_file", help="Path to the output HDF5 file")
    parser.add_argument("--train-size", type=int, required=True,
                       help="Target size for the training dataset (e.g., 20000)")
    parser.add_argument("--distance", required=True, choices=["angular", "dot", "euclidean"],
                       help="Distance metric for the dataset")
    parser.add_argument("--limit", type=int, default=100,
                       help="Number of neighbors to compute for each test query (default: 100)")
    parser.add_argument("--random-sample", action="store_true",
                       help="Randomly sample training data instead of taking first N")
    parser.add_argument("--seed", type=int, default=None,
                       help="Random seed for sampling (optional)")
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file {args.input_file} does not exist")
        return 1
    
    # Validate train size
    if args.train_size <= 0:
        print(f"Error: --train-size must be positive, got {args.train_size}")
        return 1
    
    try:
        reduce_dataset(
            args.input_file,
            args.output_file,
            args.train_size,
            args.distance,
            args.limit,
            args.random_sample,
            args.seed
        )
        return 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())



