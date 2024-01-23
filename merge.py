import csv

def merge_csv_files(file1_path, file2_path, output_path):
    with open(file1_path, 'r', newline='', encoding='utf-8') as file1:
        with open(file2_path, 'r', newline='', encoding='utf-8') as file2:
            # Read the content of both files
            file1_content = file1.readlines()
            file2_content = file2.readlines()

            # Combine the content, skipping the header of the second file
            merged_content = file1_content + file2_content[1:]

    # Write the merged content to a new file
    with open(output_path, 'w', newline='', encoding='utf-8') as output_file:
        output_file.writelines(merged_content)

# Replace 'file1.csv', 'file2.csv', and 'output_merged.csv' with your actual file paths
merge_csv_files('../la-crime.2010-2019.csv', '../la-crime.2020-present.csv', '../la-crime.2010-2023.csv')