import io

import requests


def _get_google_drive_file_list(folder_id: str, api_key: str) -> list:
    """
    Get a list of files in a Google Drive folder.
    """
    url = f"https://www.googleapis.com/drive/v3/files?key={api_key}&q='{folder_id}' in parents&fields=files(id,name)"
    response = requests.get(url)
    file_list = response.json().get("files", [])
    return file_list


def _get_google_drive_file_content(file_id: str, api_key: str) -> bytes:
    response = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}?key={api_key}&alt=media",
    )
    return response.content
