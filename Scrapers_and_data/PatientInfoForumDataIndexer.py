import requests
import os
import json
import time

from src.MetaMapWrapper import MetaMapWrapper

CURRENT_DIR = os.path.dirname(__file__)

# After 8710 every third file 1
start_pointer = 12000
end_pointer = 13500


def remove_non_ascii(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def index_json_file(file_path):
    mmw = MetaMapWrapper()

    with open(file_path) as json_file:
        data = json.load(json_file)
        failed_records = 0
        i = 0
        for post in data:
            # first index first 50000 reviews
            if i == end_pointer:
                break

            i += 1
            # skip already indexed data
            if i < start_pointer:
                continue

            # so that indexed corpus has variety of posts
            if i % 3 != 0:
                continue

            # wait for 3 seconds before every request
            # time.sleep(0.25)
            try:
                preprocessed_post = {}
                annotate_text_data = ''
                preprocessed_post['post_group'] = post['post_group']
                preprocessed_post['post_url'] = post['post_url']
                preprocessed_post['post_title'] = post['post_title']
                annotate_text_data += post['post_title']
                preprocessed_post['post_time'] = post['post_time']
                preprocessed_post['post_follow_count'] = post['post_follow_count']
                preprocessed_post['post_author'] = post['post_author']
                preprocessed_post['post_author_profile'] = post['post_author_profile']
                preprocessed_post['post_like_count'] = post['post_like_count']
                preprocessed_post['post_reply_count'] = post['post_reply_count']
                preprocessed_post['post_content'] = post['post_content']
                annotate_text_data += ' ' + post['post_content']

                comments = ''
                if 'post_comments' in post:
                    post_comments = post['post_comments']
                    for comment in post_comments:
                        extraced_comment_content = comment['extraced_comment_content']
                        extraced_comment_content = extraced_comment_content.replace("\n", " ")
                        comments += ' ' + extraced_comment_content
                    annotate_text_data += ' ' + comments
                preprocessed_post['post_comments'] = comments
                if len(annotate_text_data) > 0:
                    # important: remove non-ASCII chars as MetaMap causes tagging issue
                    annotate_text_data = remove_non_ascii(annotate_text_data)
                    extracted_data = mmw.annotate(annotate_text_data)

                    # don't index posts which does not have any symptoms mentioned in it as it does not add any value
                    # if 'symptoms' not in extracted_data:
                    #     print('Ignored indexing: ' + str(post))
                    #     continue

                    if 'symptoms' in extracted_data:
                        preprocessed_post['symptoms'] = extracted_data['symptoms']
                    if 'diseases' in extracted_data:
                        preprocessed_post['diseases'] = extracted_data['diseases']
                    if 'diagnostics' in extracted_data:
                        preprocessed_post['diagnostic_procedures'] = extracted_data['diagnostics']

                # send request to server
                r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "patient_info"},
                                  json=preprocessed_post)
                if r.status_code == 500:
                    failed_records += 1

            except Exception as exception:
                print("Exception while indexing this forum post: " + str(post))
                print('Exception message: ' + str(exception))
                failed_records += 1

    print("total number of failed requests: " + str(failed_records))

    # send final index commit request
    r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "index_commit"},
                      json={"status": "ok"})
    print("Commit status: " + str(r.status_code))


if __name__ == "__main__":
    # alternatively pass the path of crawled data file
    index_json_file(CURRENT_DIR + os.path.sep + "patient_info_forum_posts_content-1.json")
    # index_json_file(CURRENT_DIR + os.path.sep + "patient_info_forum_posts_content-2.json")
