import csv
import pandas as pd


DESCRIPTION_FEATURES = [
    "video_id", 
    "title", 
    "quality", 
    "duration", 
    "video_published_at", 
    "licensed_content", 
    "tags", 
    "caption", 
    "category_id", 
    "channel_id", 
    "channel_title", 
    "made_for_kids", 
    "privacy_status", 
    "channel_published_at"
]

DAYS = 22


def get_df(path, cols):
    with open(path, "r") as file:
        return pd.DataFrame(csv.reader(file, delimiter="\t"), columns=cols)


def get_daily_cols(metrics):
    return ["video_id"] + [metrics + '_' + str(i)for i in range(1, DAYS + 1)]


def main():
    # get data frames
    desc_df = get_df("features/21-days/descriptions.tsv", DESCRIPTION_FEATURES)

    views_df = get_df("features/21-days/views.tsv", get_daily_cols("views"))
    likes_df = get_df("features/21-days/likes.tsv", get_daily_cols("likes"))
    dislikes_df = get_df("features/21-days/dislikes.tsv", get_daily_cols("dislikes"))
    comments_df = get_df("features/21-days/comments.tsv", get_daily_cols("comments"))

    ch_subscribers_df = get_df("features/21-days/ch_subscribers.tsv", get_daily_cols("channel_subscribers"))
    ch_views_df = get_df("features/21-days/ch_views.tsv", get_daily_cols("channel_views"))
    ch_video_cnt = get_df("features/21-days/ch_video_cnt.tsv", get_daily_cols("channel_video_cnt"))

    tc_likes = get_df("features/21-days/tc_likes.tsv", get_daily_cols("top_comment_likes"))
    tc_replies = get_df("features/21-days/tc_replies.tsv", get_daily_cols("top_comment_replies"))
    tc_published = get_df("features/21-days/tc_published.tsv", get_daily_cols("top_comment_published_at"))

    # merge video metrics
    df1 = views_df.merge(likes_df, on="video_id").merge(dislikes_df, on="video_id").merge(comments_df, on="video_id")

    # merge channel metrics
    df2 = ch_video_cnt.merge(ch_views_df, on="video_id").merge(ch_subscribers_df, on="video_id")

    # merge tc metrics
    df3 = tc_likes.merge(tc_replies, on="video_id").merge(tc_published, on="video_id")

    # merge dynamic features
    dynamic_features = df1.merge(df2, on="video_id").merge(df3, on="video_id")

    # make whole dataset
    data = desc_df.merge(dynamic_features, on="video_id")
    data.to_csv("data21.tsv", sep='\t', index=False)


if __name__ == "__main__":
    main()