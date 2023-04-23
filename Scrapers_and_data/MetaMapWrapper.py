from pymetamap import MetaMap


class MetaMapWrapper(object):
    mm = MetaMap.get_instance('/Users/hemant/GithubWorkspace/MetaMap/public_mm/bin/metamap18')

    def __init__(self):
        pass

    def annotate(self, text):
        mm_request = [text]
        concepts, error = self.mm.extract_concepts(mm_request, [1, 2])
        filtered_data = {}
        symptoms = []
        diseases = []
        diagnostics = []
        for concept in concepts:
            if hasattr(concept, 'semtypes'):
                # print(concept)
                if concept.semtypes == '[sosy]':
                    # Sign or Symptom
                    # sometimes it returns symptoms as a symptom
                    if concept.preferred_name != 'Symptoms' and concept.preferred_name != 'symptoms':
                        symptoms.append(concept.preferred_name)
                elif concept.semtypes == '[dsyn]':
                    # Disease or Syndrome
                    diseases.append(concept.preferred_name)
                    pass
                elif concept.semtypes == '[diap]':
                    # Diagnostic Procedure
                    diagnostics.append(concept.preferred_name)

        if len(symptoms):
            filtered_data['symptoms'] = symptoms
        if len(diseases):
            filtered_data['diseases'] = diseases
        if len(diagnostics):
            filtered_data['diagnostics'] = diagnostics

        return filtered_data
