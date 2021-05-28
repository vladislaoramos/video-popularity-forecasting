import logging

from scrapper.setups.yt import youtube
from googleapiclient.errors import HttpError


logger = logging.getLogger(__name__)


# NOT VALID VIDEO EXAMPLE:
#     {'kind': 'youtube#videoListResponse',
#      'etag': 'YIUPVpqNjppyCWOZfL-19bLb7uk',
#      'items': [],
#      'pageInfo': {'totalResults': 0, 'resultsPerPage': 0}}

def get_video_metrics(video_id, idx):
    """
    Get video metrics for video.
    :param video_id:
    :param idx:
    :return: dict
    """
    try:
        logger.debug(f"trying to get video metrics for video #{video_id}")

        request = youtube[idx].videos().list(
            part="snippet, contentDetails, statistics, status",
            id=video_id
        )
        features = request.execute().get("items")[0]

        return features
    except IndexError:
        logger.debug(f"not valid video metrics for video #{video_id}")
        return "Not Valid"
    except KeyError:
        logger.debug(f"not valid video metrics for video #{video_id}")
        return "Not Valid"


def get_channel_metrics(channel_id, idx):
    """
    Get channel metrics for video.
    :param channel_id:
    :param idx:
    :return: dict
    """
    try:
        logger.debug(f"trying to get channel metrics for channel #{channel_id}")

        request = youtube[idx].channels().list(
            part="id, snippet, statistics",
            id=channel_id)
        features = request.execute().get("items")[0]

        return features
    except IndexError:
        logger.debug(f"not valid channel metrics for channel #{channel_id}")
        return "Not Valid"
    except KeyError:
        logger.debug(f"not valid channel metrics for channel #{channel_id}")
        return "Not Valid"
    except TypeError:
        logger.debug(f"not valid channel metrics for channel #{channel_id}")
        return "Not Valid"



def get_tc_metrics(video_id, idx):
    """
    Get top comment metrics for video.
    :param video_id:
    :param idx:
    :return: tuple
    """
    try:
        logger.debug(f"trying to get metrics for top commend of video #{video_id}")

        request = youtube[idx].commentThreads().list(
            part="snippet",
            order="relevance",
            videoId=video_id)
        response = request.execute().get("items")

        likes_cnt = 0
        published_at = None
        replies_cnt = 0

        for comment in response:
            if comment.get("snippet").get("topLevelComment").get("snippet").get("likeCount") > likes_cnt:
                likes_cnt = comment.get("snippet").get("topLevelComment").get("snippet").get("likeCount")
                published_at = comment.get("snippet").get("topLevelComment").get("snippet").get("publishedAt")
                replies_cnt = comment.get("snippet").get("totalReplyCount")

        features = {
            "likeCount": likes_cnt,
            "publishedAt": published_at,
            "totalReplyCount": replies_cnt
        }
    except HttpError:
        logger.debug(f"added data about most popular comment")

        features = {
            "likeCount": 0,
            "publishedAt": None,
            "totalReplyCount": 0
        }

    return features


class DataParser:
    def __init__(self, video_set, chunk_cnt):
        self.data = []
        self.video_set = video_set
        self.chunk_cnt = chunk_cnt
        self.chunk_size = len(self.video_set) // chunk_cnt

        # Run Parsing
        self.send_request()

    def send_request(self):
        logger.debug(f"getting chunks of video set")
        chunks = [self.video_set[i:i + self.chunk_size]
                  for i in range(0, len(self.video_set), self.chunk_size)]

        for i in range(self.chunk_cnt):
            for video_id in chunks[i]:
                # Getting features or "Not Valid"
                logger.debug(f"trying to get features or 'Not Valid' for video #{video_id}")
                video_metrics = get_video_metrics(video_id, i)
                if video_metrics == "Not Valid":
                    logger.debug(f"video #{video_id} is not valid")
                    continue
                channel_id = video_metrics.get("snippet").get("channelId")
                channel_metrics = get_channel_metrics(channel_id, i)
                tc_metrics = get_tc_metrics(video_id, i)

                # If we have "Not Valid" we need to skip this video in Handler
                self.data.append([video_metrics, channel_metrics, tc_metrics])
                logger.debug(f"features for video #{video_id} have added")

    def get_response(self):
        """
        Get parsed raw YouTube data.
        :return: [dict, dict, tuple]
        """
        # Logging: Get current (datetime) parsed YouTube data
        logger.debug(f"getting current period parsed YouTube data")

        return self.data
