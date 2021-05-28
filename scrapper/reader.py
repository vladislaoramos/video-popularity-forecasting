from scrapper.setups.yt import youtube

import os
import logging
import scrapper.setups.settings as settings


logger = logging.getLogger(__name__)


def read_file(file_path, separator=settings.TABLE_SEPARATOR):
    if os.path.isfile(file_path):
        with open(file_path, "r") as file:
            data = [line.strip().split(separator) for line in file]
        return data
    else:
        file = open(file_path, "x")
        file.close()
        return []


def get_country_codes(file_path=settings.COUNTRY_CODES_PATH):
    return [line[:2] for line in open(file_path, 'r')]


def write_video_set(video_set):
    with open(settings.VIDEO_SET_PATH, "x") as file:
        for item in video_set:
            file.write(item + '\n')


def get_video_set(codes):
    if os.path.isfile(settings.VIDEO_SET_PATH):
        return [item.strip() for item in open(settings.VIDEO_SET_PATH, "r")]
    else:
        video_set = set()

        for country in codes:
            next_page_token = None
            while True:
                request = youtube[0].videos().list(
                    part="id, contentDetails",
                    chart="mostPopular",
                    regionCode=country,
                    maxResults=50,
                    pageToken=next_page_token)
                response = request.execute()

                for item in response.get("items"):
                    video_set.add(item.get("id"))

                next_page_token = response.get('nextPageToken')

                if not next_page_token:
                    break

        video_items = list(video_set)
        write_video_set(video_items)

        return video_items


class Reader:
    def __init__(self):
        self.records = None
        self.make_structures()

    def make_structures(self):
        logger.debug(f"read records from files")

        views = read_file(settings.VIEWS_PATH)
        likes = read_file(settings.LIKES_PATH)
        dislikes = read_file(settings.DISLIKES_PATH)
        comments = read_file(settings.COMMENTS_PATH)

        channel_subscribers = read_file(settings.CHANNEL_SUBS_PATH)
        channel_views = read_file(settings.CHANNEL_VIEWS_PATH)
        channel_video = read_file(settings.CHANNEL_VIDEO_PATH)

        top_comm_likes = read_file(settings.TOP_COMM_LIKES_PATH)
        top_comm_replies = read_file(settings.TOP_COMM_REPLIES_PATH)
        top_comm_published = read_file(settings.TOP_COMM_PUBLISHED_PATH)

        self.records = {
            "views": views,
            "likes": likes,
            "dislikes": dislikes,
            "comments": comments,
            "channel_subscribers": channel_subscribers,
            "channel_views": channel_views,
            "channel_video": channel_video,
            "top_comm_likes": top_comm_likes,
            "top_comm_replies": top_comm_replies,
            "top_comm_published": top_comm_published
        }

    def get_records(self):
        return self.records
