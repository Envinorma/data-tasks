import os
from typing import Literal

from data_build.config import SECRET_DATA_FOLDER, SEED_FOLDER

ENRICHED_OUTPUT_FOLDER = os.path.join(SEED_FOLDER, 'enriched_arretes')
AM_LIST_FILENAME = os.path.join(SEED_FOLDER, 'am_list.json')
Dataset = Literal['all', 'idf', 'sample']
DataType = Literal['classements', 'installations', 'documents', 'aps']
Extension = Literal['csv', 'json']


def dataset_filename(dataset: Dataset, datatype: DataType, extenstion: Extension = 'csv') -> str:
    return os.path.join(SEED_FOLDER, f'{datatype}_{dataset}.{extenstion}')


GEORISQUES_URL = 'https://www.georisques.gouv.fr/webappReport/ws'
CQUEST_URL = 'http://data.cquest.org/icpe/commun'
GEORISQUES_DOWNLOAD_URL = 'http://documents.installationsclassees.developpement-durable.gouv.fr/commun'


GEORISQUES_DOCUMENTS_FILENAME = f'{SECRET_DATA_FOLDER}/georisques_documents.json'
INSTALLATIONS_OPEN_DATA_FILENAME = f'{SECRET_DATA_FOLDER}/icpe.geojson'
DOCUMENTS_FOLDER = f'{SECRET_DATA_FOLDER}/icpe_documents'
DGPR_INSTALLATIONS_FILENAME = f'{SECRET_DATA_FOLDER}/AP svelte/s3ic-liste-etablissements.csv'
DGPR_RUBRIQUES_FILENAME = f'{SECRET_DATA_FOLDER}/AP svelte/sic-liste-rubriques.csv'
