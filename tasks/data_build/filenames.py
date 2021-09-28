import os
from typing import Literal

from tasks.data_build.config import SECRET_DATA_FOLDER, SEED_FOLDER

ENRICHED_OUTPUT_FOLDER = os.path.join(SEED_FOLDER, 'ams')
Dataset = Literal['all', 'idf', 'sample']
DataType = Literal['classements', 'installations', 'documents', 'aps']
Extension = Literal['csv', 'json']


def dataset_object_name(dataset: Dataset, datatype: DataType, extension: Extension = 'csv') -> str:
    return f'{datatype}_{dataset}.{extension}'


GEORISQUES_URL = 'https://www.georisques.gouv.fr/webappReport/ws'
CQUEST_URL = 'http://data.cquest.org/icpe/commun'
GEORISQUES_DOWNLOAD_URL = 'http://documents.installationsclassees.developpement-durable.gouv.fr/commun'


GEORISQUES_DOCUMENTS_FILENAME = f'{SECRET_DATA_FOLDER}/georisques_documents.json'
INSTALLATIONS_OPEN_DATA_FILENAME = f'{SECRET_DATA_FOLDER}/icpe.geojson'
DOCUMENTS_FOLDER = f'{SECRET_DATA_FOLDER}/icpe_documents'
S3IC_INSTALLATIONS_FILENAME = f'{SECRET_DATA_FOLDER}/s3ic-liste-etablissements.csv'
S3IC_RUBRIQUES_FILENAME = f'{SECRET_DATA_FOLDER}/sic-liste-rubriques.csv'
