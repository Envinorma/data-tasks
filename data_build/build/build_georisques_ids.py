import json

from envinorma.data.document import DocumentType
from data_build.load import load_aps


def dump_georisques_ids(filename: str) -> None:
    ids = list(sorted([doc.georisques_id for doc in load_aps('all') if doc.type == DocumentType.AP]))
    json.dump(ids, open(filename, 'w'))
