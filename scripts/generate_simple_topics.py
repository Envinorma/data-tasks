"""Generate very simple topics for AMs. -- One shot script."""
import json
import os
from typing import Dict

from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.topics.simple_topics import add_simple_topics
from envinorma.utils import typed_tqdm, write_json

from tasks.common.config import DATA_FETCHER
from tasks.data_build.filenames import ENRICHED_OUTPUT_FOLDER
from tasks.data_build.validate.check_am import check_ams


def _load_enriched_am_list(enriched_output_folder: str) -> Dict[str, ArreteMinisteriel]:
    return {
        file_: ArreteMinisteriel.from_dict(json.load(open(os.path.join(enriched_output_folder, file_))))
        for file_ in os.listdir(enriched_output_folder)
    }


def _add_simple_topics():
    file_to_am = _load_enriched_am_list(ENRICHED_OUTPUT_FOLDER)
    for file_, am in typed_tqdm(file_to_am.items(), 'Adding topics'):
        write_json(add_simple_topics(am).to_dict(), os.path.join(ENRICHED_OUTPUT_FOLDER, file_))


def _add_simple_topics_in_am_db():
    am_metadata = DATA_FETCHER.load_all_am_metadata(False)
    for am_md in typed_tqdm(am_metadata.values(), 'Adding topics to AM.'):
        am = DATA_FETCHER.load_am(am_md.cid)
        if not am:
            continue
        DATA_FETCHER.upsert_am(am_md.cid, add_simple_topics(am))


if __name__ == '__main__':
    mode = 'db'
    if mode == 'db':
        _add_simple_topics_in_am_db()
    else:
        _add_simple_topics()
        check_ams()
