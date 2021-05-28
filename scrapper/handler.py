import datetime
import logging
import scrapper.setups.settings as settings

from shutil import copyfile


logger = logging.getLogger(__name__)


def format_tags(tags):
    """
    If video has no tags we need return the string "None".
    :param tags: list of words or None.
    :return: line of tags separating by commas or "None".
    """
    return ", ".join(tags) if tags is not None else "None"


def write_desc_to_file(features):
    """
    Write descriptions to file.
    :param features: description features as list.
    :return: just writing to file without updating any data structures.
    """
    logger.debug(f"adding descriptions")
    with open(settings.DESCRIPTIONS_PATH, "a") as file:
        file.write("\t".join([str(feature) for feature in features]) + "\n")


class DataHandler:
    def __init__(self, raw_data, records, first_day):
        self.raw_data = raw_data
        self.first_day = first_day

        # Metrics from video history
        self.views = records.get("views")
        self.likes = records.get("likes")
        self.dislikes = records.get("dislikes")
        self.comments = records.get("comments")
        self.channel_subscribers = records.get("channel_subscribers")
        self.channel_views = records.get("channel_views")
        self.channel_video = records.get("channel_video")
        self.top_comm_likes = records.get("top_comm_likes")
        self.top_comm_replies = records.get("top_comm_replies")
        self.top_comm_published = records.get("top_comm_published")

        # Run Handler
        logger.debug("Start today handling")
        self.run_handler()

    def run_handler(self):
        if self.first_day:
            self.write_desc()
        self.write_video_stats()
        self.write_channel_stats()
        self.write_tc_stats()

    def write_desc(self):
        for item in self.raw_data:
            video_features = item[0]
            video_id = video_features.get("id")
            channel_features = item[1]

            logger.debug(f"adding description about video #{video_id}")
            video_desc = [
                # video_id
                video_features.get("id"),
                # title
                video_features.get("snippet").get("title"),
                # quality
                video_features.get("contentDetails").get("definition"),
                # duration
                video_features.get("contentDetails").get("duration"),
                # description
                # is difficult to handler on this version
                # video_features.get("snippet").get("description"),
                # published_at
                video_features.get("snippet").get("publishedAt"),
                # licensed_content
                video_features.get("contentDetails").get("licensedContent"),
                # tags
                format_tags(video_features.get("snippet").get("key")),
                # caption
                video_features.get("contentDetails").get("caption"),
                # category_id
                video_features.get("snippet").get("categoryId"),
                # channel_id
                video_features.get("snippet").get("channelId"),
                # channel_title
                video_features.get("snippet").get("channelTitle"),
                # is_for_kids
                video_features.get("status").get("madeForKids"),
                # privacy_status
                video_features.get("status").get("privacyStatus"),
                # channel_description
                # channel_features.get("snippet").get("description"),
                # channel_published_at
                channel_features.get("snippet").get("publishedAt"),
            ]

            logger.debug(f"write descriptions.tsv")
            write_desc_to_file(video_desc)

    # NOT VALID CHECKING
    def write_video_stats(self):
        metrics = {
            "viewCount": self.views,
            "likeCount": self.likes,
            "dislikeCount": self.dislikes,
            "commentCount": self.comments
        }

        for key, stat in metrics.items():
            logger.debug(f"updating [{key}] metrics")
            for item in self.raw_data:
                video_features = item[0]
                video_id = video_features.get("id")
                logger.debug(f"checking whether video #{video_id} is new")
                if self.first_day:
                    logger.debug(f"video #{video_id} is new so adding its [{key}] metrics")
                    stat.append([video_id, video_features.get("statistics").get(key)])
                else:
                    logger.debug(f"video #{video_id} is in history so try to append its [{key}] metrics")
                    for video in stat:
                        if video[0] == video_id:
                            logger.debug(f"append metrics because video #{video_id} has no n-days history")
                            video.append(video_features.get("statistics").get(key))
                            break

    # NOT VALID CHECKING
    def write_channel_stats(self):
        metrics = {
            "subscriberCount": self.channel_subscribers,
            "videoCount": self.channel_video,
            "viewCount": self.channel_views,
        }

        for key, stat in metrics.items():
            logger.debug(f"updating [{key}] metrics")
            for item in self.raw_data:
                video_features = item[0]
                video_id = video_features.get("id")
                channel_features = item[1].get("statistics")
                logger.debug(f"checking whether video #{video_id} is new")
                if self.first_day:
                    logger.debug(f"video #{video_id} is new so adding its [{key}] metrics")
                    stat.append([video_id, channel_features.get(key)])
                else:
                    logger.debug(f"video #{video_id} is in history so try to append its [{key}] metrics")
                    for video in stat:
                        if video[0] == video_id:
                            logger.debug(f"video #{video_id} is in history so try to append its [{key}] metrics")
                            video.append(channel_features.get(key))
                            break

    # NOT VALID CHECKING
    def write_tc_stats(self):
        metrics = {
            "likeCount": self.top_comm_likes,
            "publishedAt": self.top_comm_published,
            "totalReplyCount": self.top_comm_replies
        }

        for key, stat in metrics.items():
            logger.debug(f"updating [{key}] metrics")
            for item in self.raw_data:
                video_features = item[0]
                video_id = video_features.get("id")
                tc_features = item[2]
                logger.debug(f"checking whether video #{video_id} is new")
                if self.first_day:
                    logger.debug(f"video #{video_id} is new so adding its [{key}] metrics")
                    stat.append([video_id, tc_features.get(key)])
                else:
                    logger.debug(f"video #{video_id} is in history so try to append its [{key}] metrics")
                    for video in stat:
                        if video[0] == video_id:
                            logger.debug(f"append metrics because video #{video_id} has no n-days history")
                            video.append(tc_features.get(key))
                            break

    def save_data(self):
        # YYYY-MM-DD:
        cur_period = "-" + str(datetime.datetime.now().isoformat()[:10]) + settings.TABLE_FORMAT
        # full date:
        # cur_period = "-" + str(datetime.datetime.now().isoformat()) + settings.TABLE_FORMAT

        path_data_pairs = [
            (settings.VIEWS_PATH, self.views),
            (settings.LIKES_PATH, self.likes),
            (settings.DISLIKES_PATH, self.dislikes),
            (settings.COMMENTS_PATH, self.comments),
            (settings.CHANNEL_SUBS_PATH, self.channel_subscribers),
            (settings.CHANNEL_VIDEO_PATH, self.channel_video),
            (settings.CHANNEL_VIEWS_PATH, self.channel_views),
            (settings.TOP_COMM_LIKES_PATH, self.top_comm_likes),
            (settings.TOP_COMM_PUBLISHED_PATH, self.top_comm_published),
            (settings.TOP_COMM_REPLIES_PATH, self.top_comm_replies)
        ]

        for path, data in path_data_pairs:
            file_path = path[:-4] + cur_period
            with open(file_path, "x") as file:
                for item in data:
                    file.write(settings.TABLE_SEPARATOR.join([str(elem) for elem in item]) + "\n")
            copyfile(file_path, path)
