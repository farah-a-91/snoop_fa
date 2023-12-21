import os
import zipfile
import json


def load_data_from_zips(folder_path):
    """
    Load data from zipped JSON files in a folder.
    """
    all_data = []

    for zip_file in os.listdir(folder_path):
        if zip_file.endswith(".zip"):
            zip_file_path = os.path.join(folder_path, zip_file)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                json_file_name = file_list[0]
                with zip_ref.open(json_file_name) as json_file:
                    json_content = json_file.read()
                all_data.append(json.loads(json_content.decode('utf-8')))

    return all_data
