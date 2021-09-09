import json
import os
from typing import Dict

from envinorma.models import ArreteMinisteriel
from envinorma.models.validate_am import check_am
from envinorma.utils import typed_tqdm

from ..filenames import ENRICHED_OUTPUT_FOLDER


def _load_enriched_am_list(enriched_output_folder: str) -> Dict[str, ArreteMinisteriel]:
    return {
        file_: ArreteMinisteriel.from_dict(json.load(open(os.path.join(enriched_output_folder, file_))))
        for file_ in os.listdir(enriched_output_folder)
    }


def check_ams() -> None:
    ams = _load_enriched_am_list(ENRICHED_OUTPUT_FOLDER)
    for filename, am in typed_tqdm(ams.items(), 'Checking AMs'):
        filename_am_id = filename.split('.json')[0]
        assert filename_am_id == am.id, f'Filename {filename} does not match AM id {am.id}'
        check_am(am)
