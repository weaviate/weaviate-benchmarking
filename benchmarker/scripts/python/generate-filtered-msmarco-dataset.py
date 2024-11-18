import pandas as pd
import numpy as np
from datasets import load_dataset
import h5py
import faiss, json, random
import os

# These filtered datasets are similar to BEIR but just use a single dataset MSMARCO
# goal is to test very high matching filters

datasets = ['msmarco', 'nq']

CORRELATED = False
DATASET_SIZE = 500000
PERCENTAGE = 0.2
QUERY_SIZE = 1000

if CORRELATED:
    # Makes the query vectors also come from msmarco as test vectors
    # are pulled from the last dataset
    datasets = ['nq', 'msmarco']

def process_dataset(dataset_name):
    df_corpus = load_dataset("Cohere/beir-embed-english-v3", f"{dataset_name}-corpus", split="train")
    if dataset_name == 'msmarco':
        embeddings = df_corpus[:int(PERCENTAGE*DATASET_SIZE)]['emb']
    else:
        embeddings = df_corpus[:int((1-PERCENTAGE)*DATASET_SIZE)]['emb']
    dataset_names = [dataset_name] * len(embeddings)
    return pd.DataFrame({'embedding': embeddings, 'dataset': dataset_names})

all_data = pd.concat([process_dataset(dataset) for dataset in datasets], ignore_index=True)
all_data['embedding'] = all_data['embedding'].apply(lambda x: np.array(x, dtype=np.float32))
# all_data = all_data.sample(frac=1, random_state=42).reset_index(drop=True)
train_size = len(all_data) - 10000
train_data = all_data[:train_size]
test_data = all_data[train_size:]

dataset_to_id = {dataset: idx for idx, dataset in enumerate(datasets)}

def generate_json_properties(category_id):
    return json.dumps({"category": str(category_id)})

def get_random_different_category(original_category, num_categories):
    categories = list(range(num_categories))
    categories.remove(original_category)
    return random.choice(categories)

train_categories = np.array([dataset_to_id[dataset] for dataset in train_data['dataset']], dtype=np.int64)
test_categories = np.array([dataset_to_id["msmarco"] for dataset in test_data['dataset']], dtype=np.int64)


train_properties = [generate_json_properties(cat) for cat in train_categories]
test_properties = [generate_json_properties(cat) for cat in test_categories]

# Generate filters
filters = []
for value in test_categories:
    filter_data = {
        "path": ["category"],
        "valueText": str(value),
        "operation": "Equal"
    }
    filters.append(json.dumps(filter_data))

train_embeddings = np.vstack(train_data['embedding'].values)
test_embeddings = np.vstack(test_data['embedding'].values)

# Build Faiss index
dimensions = train_embeddings.shape[1]
index = faiss.IndexFlatIP(dimensions)
index.add(train_embeddings)

neighbors_data = np.zeros((len(filters), 100), dtype=np.int64)
for i, filter_data in enumerate(filters):
    print(f"Brute force query {i + 1}/{len(filters)}")
    json_filter = json.loads(filter_data)
    category = int(json_filter["valueText"])
    train_indices = np.where(train_categories == category)[0]
    selector = faiss.IDSelectorArray(train_indices)
    search_params = faiss.SearchParameters(sel=selector)
    D, I = index.search(test_embeddings[i].reshape(1, -1), 100, params=search_params)
    neighbors_data[i] = I[0]

filename = f"beir-cohere-filtered-dot-{PERCENTAGE}.hdf5"

if CORRELATED:
    filename = f"beir-cohere-filtered-dot-correlated-{PERCENTAGE}.hdf5"

with h5py.File(filename, 'w') as hf:
    hf.create_dataset("train", data=train_embeddings)
    hf.create_dataset("test", data=test_embeddings)
    hf.create_dataset("train_categories", data=train_categories)
    hf.create_dataset("test_categories", data=test_categories)
    hf.create_dataset("train_properties", data=np.array(train_properties, dtype=h5py.special_dtype(vlen=str)))
    hf.create_dataset("test_properties", data=np.array(test_properties, dtype=h5py.special_dtype(vlen=str)))
    hf.create_dataset("filters", data=np.array(filters, dtype=h5py.special_dtype(vlen=str)))
    hf.create_dataset("neighbors", data=neighbors_data)

# Print file size and some information
file_size = os.path.getsize(filename)
print(f"File size: {file_size / (1024 * 1024):.2f} MB")
print(f"Train dimensions: {train_embeddings.shape}")
print(f"Test dimensions: {test_embeddings.shape}")
print(f"Neighbors dimensions: {neighbors_data.shape}")
print(f"Number of unique categories: {len(np.unique(np.concatenate([train_categories, test_categories])))}")
print(f"Sample filter: {filters[0]}")
print(f"Sample train property: {train_properties[0]}")
print(f"Sample test property: {test_properties[0]}")




