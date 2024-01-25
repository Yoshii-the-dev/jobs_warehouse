from pyflink.table import DataTypes
from pyflink.table.udf import udf
import spacy
import locationtagger
from unidecode import unidecode
import pycountry
import re
import logging 
import os
from deep_translator import GoogleTranslator

@udf(result_type = DataTypes.STRING())
def role_from_title(title):
    try:
        title = GoogleTranslator(source='auto', target='en').translate(title)
        nlp = spacy.load("en_pipeline")
        logging.debug(f"Entry title: {title}")
        doc_title = nlp(title)
        logging.debug(f"Exit doc_title.cats: {doc_title.cats}")
        role = max(doc_title.cats, key=doc_title.cats.get)
        logging.debug(f"Role: {role}")
        return role
    except ValueError:
        return 'None'
    except Exception as E:
        logging.error(f'ERROR OCCURED IN ROLE FROM TITLE: {E} | {title}')

@udf(result_type = DataTypes.STRING())
def list_of_tools(job_description):
    chunks = re.findall(r'(.{1,3000})(?=\b)', job_description)
    translated_job_description = ''
    
    for chunk in chunks:
        translated_chunk = GoogleTranslator(source='auto', target='en').translate(chunk)
        translated_job_description += translated_chunk
    nlp = spacy.load("en_pipeline")
    doc_description = nlp(translated_job_description)

    if doc_description.ents is None:
        return 'Not Found'
    return ', '.join(ent.text for ent in doc_description.ents)



@udf(result_type=DataTypes.ROW([DataTypes.FIELD('country', DataTypes.STRING()),
                                    DataTypes.FIELD('city', DataTypes.STRING())]))
def geographical_attributes(location):
    try:
        location = unidecode(location)
        country = 'Not Defined'
        city = 'Not Defined'
        try:
            logging.debug(f'OS PATH: {os.path.exists("/usr/local/lib/python3.9/site-packages/locationtagger/locationdata.db")}')
            country, city = list(locationtagger.find_locations(text = location).country_cities.items())[0]
            logging.debug(f'OS PATH: {os.path.exists("/usr/local/lib/python3.9/site-packages/locationtagger/locationdata.db")}')
            return Row(country, city[0])
        except IndexError:
            try:
                iso_country_codes = [country.alpha_2 for country in pycountry.countries]
                country_code_pattern = re.compile(r'\b(?:' + '|'.join(iso_country_codes) + r')\b')
                country_code = country_code_pattern.search(location).group(0)
                country = pycountry.countries.get(alpha_2=country_code).name
                return country, city
            except AttributeError:
                return Row(country, city)
    except Exception as E:
        logging.error(f'ERROR OCCURED IN GEOGRAPHICAL: {E}, {location}')

#@udf(result_type="INT")
#def generate_incremental_index(*fields):
#    generate_incremental_index.state = generate_incremental_index.state + 1 if hasattr(generate_incremental_index, 'state') else 1
#    return generate_incremental_index.state

        