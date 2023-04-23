import networkx as nx
import sys, getopt
import pickle
import os


CURRENT_DIR = os.path.dirname(__file__)
GRAPH_PKL_PATH = os.path.join(CURRENT_DIR, 'ppr_graph.pkl')
SYMPTOMS_PKL_PATH = os.path.join(CURRENT_DIR, 'ppr_symptoms.pkl')

class PPRSimilarSymptoms(object):


	def get_similar_symptoms(self, G, symptoms, personalized_symptom_list=[], similar_limit=5):

		result = []

		personalized_symptom_list = [i.lower() for i in personalized_symptom_list]
		personalized = dict()
		found = False
		for i in personalized_symptom_list:
			if i in symptoms:
				personalized[i] = 1
				found = True

		if not found: # If user symptom in not found in the database, returns an empty list.
			return result

		if len(personalized) > 0:
			pr = nx.pagerank(G, personalization= personalized,alpha=0.85)#, weight='weight')
		else:
			pr = nx.pagerank(G, alpha=0.85)
		rank_list = [(key, val) for key, val in pr.items()]
		rank_list = sorted(rank_list, key=lambda x: x[1], reverse=True)
		counter = 0

		for u, v in rank_list:
			if u not in personalized_symptom_list:
				if u == 'symptoms':
					continue
				counter += 1
				result.append(u)

				if counter >= similar_limit:
					break

		return result

if __name__ == "__main__":

    symptoms = []
    limit = 5
    try:
        args = getopt.getopt(sys.argv[1:], "hs:l:", ["symptoms=","limit="])
    except getopt.GetoptError:
      print('filename.py -s <comma-separated symptoms> -l <number of results required>')
      sys.exit(2)

    for arg in args[0]:
        if '-s' in arg:
            symptoms = str(arg[1]).split(",")
        if '-l' in arg:
            limit = int(arg[1])

    rank_lists = similar_symptoms(symptoms, limit)
    print("Results for entered symptoms -", ",".join(symptoms))
    [print(str(i+1) + '. ' + rank_list) for i, rank_list in enumerate(rank_lists)]
