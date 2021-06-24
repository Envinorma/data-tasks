"""Generate very simple topics for AMs.
"""
import json
import os
import re
from copy import copy
from typing import Dict, List, Optional, Tuple, Union

from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.models.structured_text import Annotations, StructuredText
from envinorma.topics.patterns import TopicName, merge_patterns, normalize
from envinorma.utils import typed_tqdm, write_json

# from tasks.data_build.load import load_ams
from tasks.data_build.filenames import ENRICHED_OUTPUT_FOLDER
from tasks.data_build.validate.check_am import check_ams

_Section = Union[StructuredText, ArreteMinisteriel]

_ONTOLOGY_RAW = {
    TopicName.DISPOSITIONS_GENERALES: ['dispositions générales'],
    TopicName.IMPLANTATION_AMENAGEMENT: ['implantation'],
    TopicName.EXPLOITATION: ['exploitation entretien'],
    TopicName.RISQUES: ['risques'],
    TopicName.EAU: ['eau', 'eaux'],
    TopicName.AIR_ODEURS: ['air'],
    TopicName.DECHETS: ['déchets', 'dechet'],
    TopicName.BRUIT_VIBRATIONS: ['bruit', 'vibrations'],
    TopicName.FIN_EXPLOITATION: ['remise en état'],
}

_ONTOLOGY = {topic: merge_patterns(list(map(normalize, patterns))) for topic, patterns in _ONTOLOGY_RAW.items()}


def _match(section_title: str) -> bool:
    return 'disposition' in section_title.lower() and 'rale' in section_title.lower()


def _detect_matching_depth(section: _Section) -> Optional[int]:
    if _match(section.title.text):
        return 0
    optional_child_matching_depths = [_detect_matching_depth(child) for child in section.sections]
    child_matching_depths = [depth for depth in optional_child_matching_depths if depth is not None]
    return None if not child_matching_depths else 1 + min(child_matching_depths)


def _detect(text: str) -> Optional[TopicName]:
    prepared_text = normalize(text)
    for topic, patterns in _ONTOLOGY.items():
        if list(re.finditer(patterns, prepared_text)):
            return topic
    return None


def _detect_and_add_topics(section: StructuredText, matching_depth: int, current_depth: int = 0) -> StructuredText:
    section = copy(section)
    topic = _detect(section.title.text) if matching_depth == current_depth else None
    section.annotations = Annotations(topic)
    section.sections = [_detect_and_add_topics(sec, matching_depth, current_depth + 1) for sec in section.sections]
    return section


def _add_topics(am: ArreteMinisteriel) -> ArreteMinisteriel:
    matching_depth = _detect_matching_depth(am)
    if matching_depth is None or matching_depth == 0:
        return am
    am = copy(am)
    am.sections = [_detect_and_add_topics(sec, matching_depth, 1) for sec in am.sections]
    return am


_Detection = Tuple[str, Optional[TopicName]]


def _extract_topic_mapping(section: _Section, matching_depth: int, current_depth: int = 0) -> List[_Detection]:
    if matching_depth == current_depth:
        if not isinstance(section, StructuredText):
            return []
        return [(section.title.text, section.annotations.topic)]
    return [elt for sec in section.sections for elt in _extract_topic_mapping(sec, matching_depth, current_depth + 1)]


def _extract_full_mapping(ams: List[ArreteMinisteriel]) -> List[_Detection]:
    return [elt for am in ams for elt in _extract_topic_mapping(am, _detect_matching_depth(am) or 0)]


def _load_enriched_am_list(enriched_output_folder: str) -> Dict[str, ArreteMinisteriel]:
    return {
        file_: ArreteMinisteriel.from_dict(json.load(open(os.path.join(enriched_output_folder, file_))))
        for file_ in os.listdir(enriched_output_folder)
    }


def _add_simple_topics():
    file_to_am = _load_enriched_am_list(ENRICHED_OUTPUT_FOLDER)
    for file_, am in typed_tqdm(file_to_am.items(), 'Adding topics'):
        write_json(_add_topics(am).to_dict(), os.path.join(ENRICHED_OUTPUT_FOLDER, file_))


if __name__ == '__main__':
    # _AMS = list(load_ams().values())

    # pairs = _extract_full_mapping(list(map(_add_topics, _AMS)))
    _add_simple_topics()
    check_ams()
