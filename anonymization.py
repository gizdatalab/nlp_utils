from gliner import GLiNER

# Entity recognition

def entity_recognizer(list_of_para, entity_list=["person", "phone number", "e-mail", "address"], anonymize=True, model="gliner_multi"): 

  """
  Recognizes and optionally anonymizes specified entities in a list of paragraphs.

  Args:
      list_of_para (list of str): A list of paragraphs in which to recognize entities.
      entity_list (list of str, optional): A list of entity types to recognize. Defaults to ["person", "phone number", "e-mail", "address"].
      anonymize (bool, optional): If True, the recognized entities in the paragraphs will be anonymized. Defaults to True.
      model (str, optional): The name of the pre-trained NER model to use. Defaults to "gliner_multi".

    Returns:
        list or dict: If anonymize is False, returns a dictionary with paragraphs as keys and lists of recognized entities as values.
                      If anonymize is True, returns a list of paragraphs with the recognized entities anonymized.
  """
  
  # Download NER model
  NER_model = GLiNER.from_pretrained(f"urchade/{model}")
  
  # Initialise an empty dictionary 
  entities_per_text = {}

  #Predict entities
  for para in list_of_para: 
    entities = NER_model.predict_entities(para, entity_list)
    entities_per_text[para] = entities
  
  # If only NER
  if not anonymize: 
    return entities_per_text
  
  # If anonymization
  else: 
    anonymized_paras = []

    # for each paragraph
    for key, value in entities_per_text.items(): 
      
      # iterate through each entity found
      anonymized_para = key
      for entity in value: 
         
        text_to_anonymize = entity["text"]
        label = entity["label"]
        anonymized_para = anonymized_para.replace(text_to_anonymize, f"[{label} removed]")
      
      anonymized_paras.append(anonymized_para)
    
    return anonymized_paras