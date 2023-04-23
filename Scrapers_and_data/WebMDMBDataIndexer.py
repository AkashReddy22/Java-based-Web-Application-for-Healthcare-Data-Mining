import requests
import os
import json
import time

from src.MetaMapWrapper import MetaMapWrapper

CURRENT_DIR = os.path.dirname(__file__)
# Stored in Google drive due to large files
DATA_DIR = CURRENT_DIR + os.path.sep + 'posts_json'


# TODO: may be index again as many post may have failed to extract symptoms
# because of non-ASCII chars in it.
# TODO: maybe the file has group notion so the post should have group name?

def remove_non_ascii(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def index_json_file(file_path):
    # print(file_path)
    mmw = MetaMapWrapper()
    with open(file_path) as json_file:
        all_data = json.load(json_file)
        failed_records = 0

        for post in all_data:
            # wait for 2 seconds before every request
            time.sleep(1)
            try:
                data_preprocessed_review = {}
                data_preprocessed_review['post_url'] = post['post_url']
                post_obj = post['post']
                data_preprocessed_review['author'] = post_obj['author']
                data_preprocessed_review['post_time'] = post_obj['post_time']
                data_preprocessed_review['post_title'] = post_obj['post_title']
                post_content = post_obj['post_content']
                post_content = post_content.replace("\n", " ")
                data_preprocessed_review['post_content'] = post_content
                data_preprocessed_review['like_count'] = int(post_obj['like_count'])
                annotate_text_data = post_obj['post_title']
                annotate_text_data += ' ' + post_content
                data_preprocessed_review['tags'] = post_obj['tags']

                # put all comments together
                response_text = ''
                responses = post['responses']
                for response in responses:
                    resp_content = response['resp_content']
                    resp_content = resp_content.replace("\n", " ")
                    response_text += ' ' + resp_content
                annotate_text_data += ' ' + response_text
                data_preprocessed_review['response_text'] = response_text

                if len(annotate_text_data) > 0:
                    # important: remove non-ASCII chars as MetaMap causes tagging issue
                    annotate_text_data = remove_non_ascii(annotate_text_data)
                    filtered_data = mmw.annotate(annotate_text_data)
                    # don't index posts which does not have any symptoms mentioned in it as it does not add any value
                    if 'symptoms' not in filtered_data:
                        print('Ignored indexing: ' + str(post))
                        continue

                    if 'symptoms' in filtered_data:
                        data_preprocessed_review['symptoms'] = filtered_data['symptoms']
                    if 'diseases' in filtered_data:
                        data_preprocessed_review['diseases'] = filtered_data['diseases']
                    if 'diagnostics' in filtered_data:
                        data_preprocessed_review['diagnostic_procedures'] = filtered_data['diagnostics']

                # send request to server
                r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "webmd_mb"},
                                  json=data_preprocessed_review)
                if r.status_code == 500:
                    failed_records += 1

            except Exception as e:
                print("Exception while indexing this post: " + str(post))
                print('Exception message: ' + str(e))
                failed_records += 1

    print("total number of failed requests: " + str(failed_records))

    # send final index commit request
    r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "index_commit"},
                      json={"status": "ok"})
    print("Commit status: " + str(r.status_code))


if __name__ == "__main__":
    grouped_files = [
        'adhd-posts.json',
        'allerrgies-posts.json',
        'arthritis-posts.json',
        'asthma-posts.json',
        'bnsd-posts.json',
        'cancer-posts.json',
        'diabetes-posts.json',
        'digestive-posts.json',
        'earnosethroat-posts.json',
        'eye-posts.json',
        'fibromyalgia-posts.json',
        'heart-posts.json',
        'hepatitisc-posts.json',
        'hiv-posts.json',
        'kidney-posts.json',
        'lupus-posts.json',
        'mental-posts.json',
        'oralhealth-posts.json',
        'osteoporosis-posts.json',
        'painmanage-posts.json',
        'sclerosis-posts.json',
        'sexhealthstd-posts.json',
        'sleep-posts.json',
        'stroke-posts.json'
    ]

    for data_file in grouped_files:
        print('indexing: ' + data_file)
        index_json_file(DATA_DIR + os.path.sep + data_file)

    print("All the data files has been indexed!")