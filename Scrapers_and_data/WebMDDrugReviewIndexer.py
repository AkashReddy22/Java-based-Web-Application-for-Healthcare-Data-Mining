import requests
import os
import json
import time

from src.MetaMapWrapper import MetaMapWrapper

CURRENT_DIR = os.path.dirname(__file__)

start_pointer = 60001
end_pointer = 65000


def remove_non_ascii(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def index_json_file(file_path):
    mmw = MetaMapWrapper()
    with open(file_path) as json_file:
        data = json.load(json_file)
        failed_records = 0
        i = 0
        for review in data:
            # first index first 50000 reviews
            if i == end_pointer:
                break

            i += 1
            # skip already indexed data
            if i < start_pointer:
                continue

            # wait for 3 seconds before every request
            time.sleep(0.1)
            try:
                preprocessed_data_review = {}
                preprocessed_data_review['drug_name'] = review['drug_name']
                preprocessed_data_review['drug_detail_page'] = review['drug_detail_page']
                preprocessed_data_review['drug_review_page'] = review['drug_review_page']
                preprocessed_data_review['health_condition_name'] = review['health_condition_name']
                preprocessed_data_review['timestamp'] = review['timestamp']
                preprocessed_data_review['reviewer_name'] = review['reviewer_name']
                age_range = review['patient_age_range']
                if age_range == 'unknown':
                    preprocessed_data_review['patient_age_start'] = int(-1)
                    preprocessed_data_review['patient_age_end'] = int(-1)
                else:
                    patient_age_parts = review['patient_age_range'].split('-')
                    preprocessed_data_review['patient_age_start'] = int(patient_age_parts[0])
                    preprocessed_data_review['patient_age_end'] = int(patient_age_parts[1])

                preprocessed_data_review['patient_gender'] = review['patient_gender']
                preprocessed_data_review['treatment_duration'] = review['treatment_duration']
                preprocessed_data_review['reviewer_category'] = review['reviewer_category']
                review_comment = review['review_comment']
                preprocessed_data_review['review_comment'] = review_comment
                if len(review_comment) > 0:
                    # important: remove non-ASCII chars as MetaMap causes tagging issue
                    review_comment = remove_non_ascii(review_comment)
                    extracted_data = mmw.annotate(review_comment)
                    if 'symptoms' in extracted_data:
                        preprocessed_data_review['symptoms'] = extracted_data['symptoms']
                    if 'diseases' in extracted_data:
                        preprocessed_data_review['diseases'] = extracted_data['diseases']
                    if 'diagnostics' in extracted_data:
                        preprocessed_data_review['diagnostic_procedures'] = extracted_data['diagnostics']

                preprocessed_data_review['num_of_people_found_useful'] = review['num_of_people_found_useful']
                preprocessed_data_review['effectiveness_rating'] = int(review['effectiveness_rating'])
                preprocessed_data_review['ease_of_use_rating'] = int(review['ease_of_use_rating'])
                preprocessed_data_review['satisfaction_rating'] = int(review['satisfaction_rating'])

                # send request to server
                r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "drug_review"},
                                  json=preprocessed_data_review)
                if r.status_code == 500:
                    failed_records += 1

            except Exception as e:
                print("Exception while indexing this review: " + str(review))
                print('Exception message: ' + str(e))
                failed_records += 1

    print("total number of failed requests: " + str(failed_records))

    # send final index commit request
    r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "index_commit"},
                      json={"status": "ok"})
    print("Commit status: " + str(r.status_code))


if __name__ == "__main__":
    # alternatively pass the path of crawled data file
    index_json_file(CURRENT_DIR + os.path.sep + "webmd_drugs_reviews.json")
