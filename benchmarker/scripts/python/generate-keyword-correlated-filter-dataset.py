import argparse
import h5py
import jsonlines
import numpy as np
import faiss
import nltk
from nltk.tokenize import word_tokenize
import logging
from tqdm import tqdm
import dspy
import openai

import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('punkt')

def tokenize_and_remove_stop_words(text):
    # Convert text to lowercase
    text = text.lower()
    
    # Tokenize the text into words
    tokens = word_tokenize(text)
    
    # Define custom stopwords
    stop_words = {'and', 'the', 'or', 'a', 'an', 'in', 'on', 'at', 'of', 'to', 'for', 'with', 'by', 'from', 'up', 'down', 'in', 'out', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now'}
    
    # Remove stop words
    filtered_tokens = [token for token in tokens if token not in stop_words]
    
    return filtered_tokens

def main(args):
    print("Reading the JSONL dataset...")
    data = []
    with jsonlines.open(args.input_file) as reader:
        for obj in tqdm(reader, desc="Reading objects"):
            data.append(obj)
    print(f"Read {len(data)} objects from the dataset.")

    print("Finding keyword...")
    keyword_freq = {}
    for obj in tqdm(data, desc="Tokenizing and counting frequencies"):
        tokens = tokenize_and_remove_stop_words(obj["title"] + " " + obj["raw"])
        for token in tokens:
            keyword_freq[token] = keyword_freq.get(token, 0) + 1

    keyword = None

    if args.dspy_check:
    
        class AsessKeywordQuality(dspy.Signature):
            """Assess whether this keyword would make for an interesting filter. For example keywords like `lion` or `ocean` are great, but generic words like `case` are not great."""

            keyword: str = dspy.InputField()
            keyword_quality: bool = dspy.OutputField(desc="Only respond with this value.")
    
        # would be better to format `lion`: true | `ocean`: true | `case`: false as examples

        gpt_4 = dspy.OpenAI(model="gpt-4")
        openai.api_key = args.openai_api_key
        dspy.settings.configure(lm=gpt_4)

        assess_keyword_program = dspy.TypedPredictor(AsessKeywordQuality)
    

        for word, freq in keyword_freq.items():
            if args.min_frequency <= freq <= args.max_frequency:
                keyword_quality = assess_keyword_program(keyword=word).keyword_quality
                print(f"Judged keyword quality {word} to be {str(keyword_quality)}\n")
                if keyword_quality:
                    keyword = word
                    break
    
    else:
        for word, freq in keyword_freq.items():
            if args.min_frequency <= freq <= args.max_frequency:
                keyword = word
                break

    if keyword is None:
        raise ValueError(f"No keyword found within the specified frequency range ({args.min_frequency} - {args.max_frequency})")
    print(f"Found keyword: {keyword}")


    print("Adding the 'contains_keyword' filter...")
    contains_keyword = np.zeros(len(data), dtype=bool)
    for i, obj in enumerate(tqdm(data, desc="Checking keyword presence")):
        if keyword in obj["title"] or keyword in obj["raw"]:
            contains_keyword[i] = True
    print("Finished adding the 'contains_keyword' filter.")

    print("Creating separate lists for objects with and without the keyword...")
    data_with_keyword = [obj for obj, has_keyword in zip(data, contains_keyword) if has_keyword]
    data_without_keyword = [obj for obj, has_keyword in zip(data, contains_keyword) if not has_keyword]
    print(f"Number of objects containing the keyword: {len(data_with_keyword)}")
    print(f"Number of objects not containing the keyword: {len(data_without_keyword)}")

    print("Building FAISS index for objects with the keyword...")
    vectors_with_keyword = np.array([obj["vector"] for obj in data_with_keyword], dtype=np.float32)
    index = faiss.IndexFlatIP(vectors_with_keyword.shape[1])
    index.add(vectors_with_keyword)
    print("Finished building FAISS index.")

    print("Calculating nearest neighbors for objects with the keyword...")
    nearest_neighbors = []
    for obj in tqdm(data_with_keyword, desc="Calculating nearest neighbors"):
        query_vector = np.array(obj["vector"], dtype=np.float32).reshape(1, -1)
        distances, indices = index.search(query_vector, args.num_neighbors)
        nearest_neighbors.append(indices[0].tolist())
    print("Finished calculating nearest neighbors.")

    print("Storing the data in HDF5 format...")
    with h5py.File(args.output_file, "w") as f:
        f.create_dataset("id", data=[obj["id"] for obj in data])
        f.create_dataset("url", data=[obj["url"] for obj in data], dtype=h5py.special_dtype(vlen=str))
        f.create_dataset("title", data=[obj["title"] for obj in data], dtype=h5py.special_dtype(vlen=str))
        f.create_dataset("raw", data=[obj["raw"] for obj in data], dtype=h5py.special_dtype(vlen=str))
        f.create_dataset("vector", data=[obj["vector"] for obj in data])
        f.create_dataset("contains_keyword", data=contains_keyword)
        nearest_neighbors_array = [np.array(sublist, dtype=np.int32) for sublist in nearest_neighbors]
        f.create_dataset("nearest_neighbors", data=nearest_neighbors_array, dtype=h5py.special_dtype(vlen=np.dtype(np.int32)))
    print(f"Successfully generated filtered dataset with keyword: {keyword}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate filtered dataset")
    parser.add_argument("input_file", help="Path to the input JSONL file (tested with `sphere.1M.jsonl`)")
    parser.add_argument("output_file", help="Path to the output HDF5 file")
    parser.add_argument("--min-frequency", type=int, default=40000, help="Minimum frequency of the keyword (default: 40000)")
    parser.add_argument("--max-frequency", type=int, default=50000, help="Maximum frequency of the keyword (default: 50000)")
    parser.add_argument("--num-neighbors", type=int, default=100, help="Number of nearest neighbors to retrieve (default: 100)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    parser.add_argument("--dspy-check", default=False, help="Enable dspy keyword quality check (default: True)")
    parser.add_argument("--openai-api-key", help="OpenAI API key")

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    main(args)
