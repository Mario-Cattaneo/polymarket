import os
import pandas as pd
import ast
from collections import defaultdict

def select_k_representative_labels(k, input_csv_path, output_csv_path, stop_labels=None):
    """
    Selects k representative labels from a CSV file using a greedy set cover algorithm,
    after filtering out a list of stop labels.

    Args:
        k (int): The number of representative labels to select.
        input_csv_path (str): The path to the input CSV file.
        output_csv_path (str): The path to save the output CSV file.
        stop_labels (set): A set of labels to ignore during the selection process.
    """
    try:
        # 1. Read and process the data
        print(f"Reading data from {input_csv_path}...")
        df = pd.read_csv(input_csv_path)
        df['tag_label_list'] = df['tag_label'].apply(ast.literal_eval)

        # 2. Map each label to the set of rows it appears in
        label_to_rows = defaultdict(set)
        for index, row in df.iterrows():
            for label in row['tag_label_list']:
                label_to_rows[label].add(index)

        # 3. NEW: Filter out the stop labels before selection
        if stop_labels is None:
            stop_labels = set()
        
        print(f"\nOriginal number of unique labels: {len(label_to_rows)}")
        available_labels = set(label_to_rows.keys()) - stop_labels
        print(f"Labels available for selection after filtering: {len(available_labels)}")
        print(f"Ignoring the following labels: {stop_labels}\n")

        # 4. Implement the greedy algorithm to select k labels
        print(f"Selecting {k} representative labels from the filtered list...")
        selected_labels = []
        covered_rows = set()

        for i in range(k):
            best_label = None
            max_new_coverage = -1

            # Find the label that covers the most new rows from the available pool
            for label in available_labels:
                new_rows = label_to_rows[label] - covered_rows
                if len(new_rows) > max_new_coverage:
                    max_new_coverage = len(new_rows)
                    best_label = label
            
            if best_label:
                selected_labels.append(best_label)
                covered_rows.update(label_to_rows[best_label])
                available_labels.remove(best_label) # Ensure we don't pick it again
                print(f"  ({i+1}/{k}) Selected '{best_label}', covering {max_new_coverage} new rows.")
            else:
                print("No more labels with new coverage to select.")
                break
        
        # 5. Save the results to the output CSV
        print(f"\nSaving the {len(selected_labels)} selected labels to {output_csv_path}...")
        output_df = pd.DataFrame(selected_labels, columns=['representative_label'])
        output_df.to_csv(output_csv_path, index=False)
        
        total_rows = len(df)
        coverage_percentage = (len(covered_rows) / total_rows) * 100 if total_rows > 0 else 0
        print("Done.")
        print(f"The selected labels cover {len(covered_rows)} out of {total_rows} rows ({coverage_percentage:.2f}%).")

    except FileNotFoundError:
        print(f"Error: The file was not found at {input_csv_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    # --- Configuration ---
    # Set the desired number of representative labels (k)
    K_LABELS = 12  # You can experiment with this number

    # Define a set of labels to ignore because they are too generic or not topic-related.
    # You can add more labels to this set as you see fit!
    STOP_LABELS = {'All', 'Trump', 'NFL', 'Mentions', 'Trump Presidency', 'Games', 'Soccer'} 

    # --- Define file paths from environment variables ---
    output_dir = os.getenv("POLY_CSV", ".")
    INPUT_FILE = os.path.join(output_dir, "gamma_events.csv")
    OUTPUT_FILE = os.path.join(output_dir, "labels.csv")

    # --- Run the label selection process with the stop list ---
    select_k_representative_labels(K_LABELS, INPUT_FILE, OUTPUT_FILE, stop_labels=STOP_LABELS)