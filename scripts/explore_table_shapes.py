"""Study table shapes in AM for extraction in inspection sheets.
"""
from collections import Counter
from typing import Dict, List, Union

from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.models.structured_text import StructuredText
from envinorma.models.text_elements import Table

from tasks.data_build.load import load_ams

_Section = Union[StructuredText, ArreteMinisteriel]


def _extract_tables(section: _Section) -> List[Table]:
    section_tables = (
        [al.table for al in section.outer_alineas if al.table is not None]
        if isinstance(section, StructuredText)
        else []
    )
    return section_tables + [table for section in section.sections for table in _extract_tables(section)]


def _extract_table_lengths_counter(ams: List[ArreteMinisteriel]) -> Dict[int, int]:
    tables = [table for am in ams for table in _extract_tables(am)]
    return Counter([len(table.rows) for table in tables])


def _max_rowspan(table: Table) -> int:
    return max([cell.rowspan for row in table.rows for cell in row.cells])


def _extract_table_max_rowspan_counter(ams: List[ArreteMinisteriel]) -> Dict[int, int]:
    tables = [table for am in ams for table in _extract_tables(am)]
    return Counter([_max_rowspan(table) for table in tables])


if __name__ == '__main__':
    _AMS = list(load_ams().values())
    _extract_table_lengths_counter(_AMS)
    _extract_table_max_rowspan_counter(_AMS)
