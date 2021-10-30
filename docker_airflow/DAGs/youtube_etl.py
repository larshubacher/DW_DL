from googleapiclient.discovery import build
import pandas as pd
import time

#search_term = "Bitcoin"
#api_key = "AIzaSyCrbo3YbznPw7nFYpx6Ru3Y7k__ZzdgaGo"
#youtube = build("youtube", "v3", developerKey=api_key)
#number_of_videos = 50


def get_video_list(youtube):
    video_list = []
    request = youtube.search().list(q=search_term, part="snippet", type="video", maxResults=50,
                                    order="date")  ## order --> Resources are sorted in reverse chronological order based on the date they were created
    response = request.execute()

    time.sleep(5)

    while len(video_list) < number_of_videos:  ## kleiner als muss in 50er schritten sein.

        for video in response["items"]:
            video_list.append(video["id"]["videoId"])
            video_title = video["snippet"]["title"]  ##video title
            upload_date = video["snippet"]["publishedAt"]

        if len(video_list) < number_of_videos:
            request = youtube.search().list(q=search_term, part="snippet", type="video", maxResults=50,pageToken="nextPageToken")
        else:
            break

    ti.xcom_push(key="video_list", value=video_list)
    return video_list



#video_list = get_video_list(youtube)
#print(video_list)
#
#
def get_video_details(youtube, video_list, ti):

    stats_list = []
    video_list = ti.xcom_pull(key="video_list", task_ids="get_video_list")
    for i in range(0, len(video_list), 50):  ## range(start, end, steps)
        request = youtube.videos().list(
            part="snippet, contentDetails, statistics",
            id=video_list[i:i + 50])

        data = request.execute()

        for video in data["items"]:
            title = video["snippet"]["title"]
            published = video["snippet"]["publishedAt"]
            view_count = video["statistics"].get("viewCount", 0)
            like_count = video["statistics"].get("likeCount", 0)
            dislike_count = video["statistics"].get("dislikeCount", 0)
            comment_count = video["statistics"].get("commentCount", 0)

            stats_dictionary = dict(title=title,
                                    published=published,
                                    view_count=view_count,
                                    like_count=like_count,
                                    dislike_count=dislike_count,
                                    comment_count=comment_count)

            stats_list.append(stats_dictionary)


    return stats_list

#video_data = get_video_details(youtube, video_list)
#video_data
