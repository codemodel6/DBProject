import requests
import json

url = 'https://7009-14-7-188-17.ngrok-free.app/files'
def get_file(filename):
    response = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

# def read_data_json(file):
#     with open(file, 'r', encoding = 'utf-8') as f:
#         data = json.load(f)
#     return data

def read_json_data(file_path):
    with open(file_path, encoding='UTF-8') as f:
        json_data = json.load(f)
    return json_data