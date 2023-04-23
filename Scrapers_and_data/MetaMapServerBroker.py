from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib import parse
from sys import argv
import json
from src.MetaMapWrapper import MetaMapWrapper
from src.PPRSimilarSymptoms import PPRSimilarSymptoms
import networkx as nx
import pickle
import os

CURRENT_DIR = os.path.dirname(__file__)
GRAPH_PKL_PATH = os.path.join(CURRENT_DIR, 'ppr_graph.pkl')
SYMPTOMS_PKL_PATH = os.path.join(CURRENT_DIR, 'ppr_symptoms.pkl')


class Server(BaseHTTPRequestHandler):
    mmw = MetaMapWrapper()
    # PPR
    G = nx.Graph()
    symptoms = set()
    with open(SYMPTOMS_PKL_PATH, 'rb') as f:
        symptoms = pickle.load(f)
    with open(GRAPH_PKL_PATH, 'rb') as f:
        G = pickle.load(f)
    ss = PPRSimilarSymptoms()

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        print("Received a request..")
        self._set_headers()
        annotated_query = {}
        print(self.request)
        o = parse.urlparse(self.path)
        parsed_params = parse.parse_qs(o.query)
        search_query_str = parsed_params['query']
        extracted_data = self.mmw.annotate(search_query_str)
        if 'symptoms' in extracted_data:
            annotated_query['symptoms'] = extracted_data['symptoms']
        if 'diseases' in extracted_data:
            annotated_query['diseases'] = extracted_data['diseases']
        if 'diagnostics' in extracted_data:
            annotated_query['diagnostic_procedures'] = extracted_data['diagnostics']

        # PPR to fetch similar symtpoms
        # args - <Graph(loaded from pkl), symptoms(loaded from pkl), list of user symptoms, limit of returned symptoms(default-5)>
        # returns - list of similar symptoms
        if 'symptoms' in annotated_query:
            extracted_data['symptoms_suggestion'] = self.ss.get_similar_symptoms(self.G, self.symptoms, annotated_query['symptoms'])

        encoded = json.dumps(extracted_data).encode()
        self.wfile.write(encoded)


def run(server_class=HTTPServer, handler_class=Server, port=8008):
    # server_address = ('', port)
    server_address = ('localhost', 8008)
    handler_class.protocol_version = "HTTP/1.0"
    httpd = server_class(server_address, handler_class)

    print('Starting httpd on port %d...' % port)
    httpd.serve_forever()


if __name__ == "__main__":
    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()