import requests
import os
import json
import time

from src.MetaMapWrapper import MetaMapWrapper

CURRENT_DIR = os.path.dirname(__file__)
MAYO_CLINIC_HOME_PAGE = 'https://www.mayoclinic.org'

start_pointer = 0
# 1182 max
end_pointer = 1182


def remove_non_ascii(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def index_json_file(file_path):
    mmw = MetaMapWrapper()

    with open(file_path) as json_file:
        data = json.load(json_file)
        failed_records = 0
        for post_id in range(start_pointer, end_pointer):
            # wait for 3 seconds before every request
            time.sleep(0.25)
            try:
                preprocessed_data = {}

                page_data = data[str(post_id)]
                link = page_data['link']
                link = link.replace('\\', '')

                temp = link[len('diseases-conditions')+2:]
                health_condition = temp[0: temp.find('/')]
                preprocessed_data['health_condition'] = health_condition
                preprocessed_data['page_url'] = MAYO_CLINIC_HOME_PAGE + link
                if 'symptoms' in page_data:
                    symptoms_text = ''
                    symptoms = page_data['symptoms']
                    for symptom in symptoms:
                        symptoms_text += ' ' + symptom
                    preprocessed_data['symptoms_text'] = symptoms_text

                    # important: remove non-ASCII chars as MetaMap causes tagging issue
                    symptoms_text = remove_non_ascii(symptoms_text)
                    filtered_data = mmw.annotate(symptoms_text)
                    if 'symptoms' in filtered_data:
                        preprocessed_data['symptoms'] = filtered_data['symptoms']
                    if 'diseases' in filtered_data:
                        preprocessed_data['diseases'] = filtered_data['diseases']
                    if 'diagnostics' in filtered_data:
                        preprocessed_data['diagnostic_procedures'] = filtered_data['diagnostics']

                # send request to server
                r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "mayo_clinic"},
                                  json=preprocessed_data)
                if r.status_code == 500:
                    failed_records += 1

            except Exception as exception:
                print("Exception while indexing this page: " + str(page_data))
                print('Exception message: ' + str(exception))
                failed_records += 1

    print("total number of failed requests: " + str(failed_records))

    # send final index commit request
    r = requests.post('http://localhost:8080/healthcare_mining/index', params={"type": "index_commit"},
                      json={"status": "ok"})
    print("Commit status: " + str(r.status_code))


if __name__ == "__main__":
    # alternatively pass the path of crawled data file
    index_json_file(CURRENT_DIR + os.path.sep + "mayo_symptoms_data.json")
