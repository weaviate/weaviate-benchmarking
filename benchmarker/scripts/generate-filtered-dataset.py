import h5py
import numpy as np
import faiss
import json
import random
import argparse

def generate_categorical_text(distribution, num_categories):
    midpoint = num_categories // 2
    if distribution == "uniform":
        return random.randint(1, num_categories)
    else:
        return int(np.clip(np.random.normal(loc=midpoint, scale=midpoint / 2), 1, num_categories))
    
def generate_json_properties(name, value):
    properties = {
        name: str(value),
    }
    return json.dumps(properties)


def main(args):
    original = h5py.File(args.original_file, "r")

    print(f"Train dimensions: {original['train'].shape}")
    print(f"Test dimensions: {original['test'].shape}")
    print(f"Neighbors dimensions: {original['neighbors'].shape}")

    target = h5py.File(args.target_file, "w")
    target.create_dataset("train", data=original["train"][:])
    target.create_dataset("test", data=original["test"][:])

    # name = f"{args.distribution}Text{args.categories}"
    name = "category"
    print(f"Building categorical filters for {name}...")

    train_categories = [generate_categorical_text(args.distribution, args.categories) for _ in range(original["train"].shape[0])]
    test_categories = [generate_categorical_text(args.distribution, args.categories) for _ in range(original["test"].shape[0])]

    train_properties = [generate_json_properties(name, category) for category in train_categories]
    test_properties = [generate_json_properties(name, category) for category in test_categories]

    target.create_dataset("train_categories", data=np.array(train_categories, dtype=np.int64))
    target.create_dataset("test_categories", data=np.array(test_categories, dtype=np.int64))

    target.create_dataset("train_properties", data=np.array(train_properties, dtype=h5py.special_dtype(vlen=str)))
    target.create_dataset("test_properties", data=np.array(test_properties, dtype=h5py.special_dtype(vlen=str)))

    filters = []
    for value in test_categories:
        filter_data = {
            "path": [name],
            "valueText": str(value),
            "operation": "Equal"
        }
        filters.append(json.dumps(filter_data))

    target.create_dataset("filters", data=np.array(filters, dtype=h5py.special_dtype(vlen=str)))

    print(f"filter[0]: {filters[0]}")
    print(f"filter[1]: {filters[1]}")
    print(f"train_properties[0]: {train_properties[0]}")
    print(f"train_properties[1]: {train_properties[1]}")

    dimensions = original["train"].shape[1]

    print(f"Building flat index for {dimensions} dimensions...")
    index = faiss.IndexFlatIP(dimensions)
    index.add(original["train"][:])

    neighbors_data = np.zeros((len(filters), args.limit), dtype=np.int64)

    for i, filter_data in enumerate(filters):
        json_filter = json.loads(filter_data)
        category = int(json_filter["valueText"])
        train_indices = [j for j in range(len(train_categories)) if train_categories[j] == category]
        selector = faiss.IDSelectorArray(np.array(train_indices, dtype=np.int64))
        search_params = faiss.SearchParameters(sel=selector)
        D, I = index.search(original["test"][:][i].reshape(1, -1), args.limit, params=search_params)
        neighbors_data[i] = I[0]

        if i == 0:
            print(f"Test query {i}:")
            print(f"Category: {category}")
            print(f"Length of train_indices: {len(train_indices)}")
            print(f"Distances: {D.shape}")
            print(f"Indices: {I.shape}")
            print(f"Nearest neighbors: {I[0]}")
            print()

            print(f"Distance[0]: {D[0][0]}")
            print(f"Distance[1]: {D[0][1]}")

    target.create_dataset("neighbors", data=neighbors_data)
    target.close()
    print("Successfully generated filtered dataset")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate filtered dataset")
    parser.add_argument("original_file", help="Path to the original HDF5 file")
    parser.add_argument("target_file", help="Path to the target HDF5 file")
    parser.add_argument("--distribution", default="normal", choices=["uniform", "normal"], help="Distribution type (default: normal)")
    parser.add_argument("--categories", type=int, default=10, help="Number of categories for uniform distribution or clipping range for normal distribution (default: 20)")
    parser.add_argument("--limit", type=int, default=100, help="Limit of each query (default: 100)")

    args = parser.parse_args()
    main(args)