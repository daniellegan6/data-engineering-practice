import os
import glob
import csv
import json

def flatten_json(data: dict[str, any], sep: str = '_') -> dict[str, str]:
    
    flattened = {}

    def _flatten(current, name=''):
        if isinstance(current, dict):
            for key, value in current.items():
                new_name = f"{name}{sep}{key}" if name else key
                _flatten(value, new_name)
        elif isinstance(current, list):
            i = 0
            for val in current:
                new_name = f"{name}{sep}{i}" if name else f"{key}{sep}{i}"
                _flatten(val, new_name)
                i += 1
        else: flattened[name] = current

    _flatten(data)
    return flattened


def json_to_csv(json_file):

    csv_file = json_file.rsplit('.', 1)[0] + '.csv'
    
    with open(json_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error reading json file {json_file}: {e}")
            return
    
    flattened_data = flatten_json(data)
    # print(flattened_data)
    headers = list(flattened_data.keys())

    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerow(flattened_data)

    print(f"Successfully converted {json_file} to {csv_file}")


def main():
    # 1. Crawl the `data` directory with `Python` and identify all the `json` files.
    # 2. Load all the `json` files.
    # 3. Flatten out the `json` data structure.
    # 4. Write the results to a `csv` file, one for one with the json file, including the header names.
    data_dir = os.path.join(os.path.dirname(__file__), 'data') 
    json_pattern = os.path.join(data_dir, '**', '*.json')
    json_files = glob.glob(json_pattern, recursive=True)

    if not json_files:
        print("No JSON files found")
        return
    
    print(f"Found {len(json_files)} Json files to process")

    for json_file in json_files:
        print(f"\nProcessing {json_file}")
        json_to_csv(json_file)


if __name__ == "__main__":
    main()
