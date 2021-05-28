from googleapiclient.discovery import build


API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
API_KEYS_PATH = "scrapper/setups/api-keys.txt"
API_KEYS_COUNT = 5


def get_api_keys():
    return [key for key in open(API_KEYS_PATH, "r")]


def yt_build(keys):
    return [build(API_SERVICE_NAME, API_VERSION, developerKey=item) for item in keys]


api_keys = get_api_keys()
youtube = yt_build(api_keys)
