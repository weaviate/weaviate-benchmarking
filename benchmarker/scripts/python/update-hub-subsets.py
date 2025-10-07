#!/usr/bin/env python3
"""
Script to update README.md with subset configurations for Hugging Face Hub datasets.
This script lists all top-level folders in a HF dataset and updates the README.md
header section to correctly configure all subsets.
"""

import argparse
import yaml
from pathlib import Path
from typing import List, Optional
from huggingface_hub import HfApi, hf_hub_download, list_repo_files


def list_dataset_subsets(repo_id: str, token: Optional[str] = None) -> List[str]:
    """
    List all top-level folders (subsets) in a Hugging Face dataset repository.
    
    Args:
        repo_id: Repository ID for the Hugging Face Hub dataset
        token: Hugging Face token (optional)
        
    Returns:
        List of subset names (top-level folder names)
    """
    api = HfApi(token=token)
    
    try:
        # List all files in the repository
        files = list_repo_files(repo_id=repo_id, repo_type="dataset", token=token)
        
        # Extract top-level folders (subsets)
        subsets = set()
        for file_path in files:
            # Split path and get the first part (subset name)
            parts = file_path.split('/')
            if len(parts) > 1:
                subset_name = parts[0]
                # Only include folders that contain dataset files
                if any(part.endswith('.parquet') for part in parts):
                    subsets.add(subset_name)
        
        return sorted(list(subsets))
        
    except Exception as e:
        print(f"Error listing subsets from {repo_id}: {e}")
        return []


def generate_config_yaml(subsets: List[str]) -> str:
    """
    Generate YAML configuration header for README.md.
    
    Args:
        subsets: List of subset names
        
    Returns:
        YAML string for the configs section
    """
    configs = []
    for subset in subsets:
        configs.append({
            "config_name": subset,
            "data_dir": subset
        })
    
    yaml_content = {
        "configs": configs
    }
    
    return yaml.dump(yaml_content, default_flow_style=False, sort_keys=False)


def update_readme_header(repo_id: str, subsets: List[str], token: Optional[str] = None) -> None:
    """
    Update or create README.md with subset configurations.
    
    Args:
        repo_id: Repository ID for the Hugging Face Hub dataset
        subsets: List of subset names
        token: Hugging Face token (optional)
    """
    api = HfApi(token=token)
    
    # Generate the YAML config header
    yaml_content = generate_config_yaml(subsets)
    
    # Create the header section
    header = f"---\n{yaml_content}---\n\n"
    
    try:
        # Try to download existing README.md
        try:
            readme_path = hf_hub_download(
                repo_id=repo_id,
                filename="README.md",
                repo_type="dataset",
                token=token
            )
            
            # Read existing content
            with open(readme_path, 'r', encoding='utf-8') as f:
                existing_content = f.read()
            
            # Check if README already has a YAML header
            if existing_content.startswith('---'):
                # Find the end of the existing YAML header
                lines = existing_content.split('\n')
                end_yaml_idx = 1
                for i, line in enumerate(lines[1:], 1):
                    if line.strip() == '---':
                        end_yaml_idx = i + 1
                        break
                
                # Replace the header section
                new_content = header + '\n'.join(lines[end_yaml_idx:])
            else:
                # No existing header, prepend the new header
                new_content = header + existing_content
                
        except Exception:
            # README.md doesn't exist, create a new one
            new_content = header
        
        # Upload the updated README.md
        api.upload_file(
            path_or_fileobj=new_content.encode('utf-8'),
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo="README.md",
            commit_message=f"Update README.md with subset configurations: {', '.join(subsets)}"
        )
        
        print(f"Successfully updated README.md for {repo_id}")
        print(f"Configured subsets: {', '.join(subsets)}")
        
    except Exception as e:
        print(f"Error updating README.md for {repo_id}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Update README.md with subset configurations for Hugging Face Hub datasets"
    )
    parser.add_argument("--repo-id", required=True, help="Repository ID for the Hugging Face Hub dataset")
    parser.add_argument("--token", help="Hugging Face token (optional)")
    
    args = parser.parse_args()
    
    try:
        # List all subsets in the dataset
        print(f"Listing subsets in {args.repo_id}...")
        subsets = list_dataset_subsets(args.repo_id, args.token)
        
        if not subsets:
            print("No subsets found in the dataset.")
            return 1
        
        print(f"Found subsets: {', '.join(subsets)}")
        
        # Update README.md with subset configurations
        print("Updating README.md...")
        update_readme_header(args.repo_id, subsets, args.token)
        
        print(f"Successfully updated README.md for {args.repo_id}")
        print(f"Dataset URL: https://huggingface.co/datasets/{args.repo_id}")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
