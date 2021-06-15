import math
from datetime import date

import pytest
from envinorma.models import DateParameterDescriptor, VersionDescriptor

from data_build.validate.check_am import _assert_is_partition_matrix, _is_a_partition


def test_is_partition():
    assert _is_a_partition([(-math.inf, math.inf)])
    assert _is_a_partition([(-math.inf, 1), (1, math.inf)])
    assert _is_a_partition([(-math.inf, 1), (1, 10), (10, math.inf)])
    assert _is_a_partition([(-math.inf, 1), (1, 10), (10, 20), (20, math.inf)])
    assert _is_a_partition([(20, math.inf), (-math.inf, 1), (1, 10), (10, 20)])
    assert not _is_a_partition([(-math.inf, 1), (1, 20), (10, 20), (20, math.inf)])
    assert not _is_a_partition([(-math.inf, 1), (1, 10), (10, 20), (20, 30)])
    assert not _is_a_partition([])


def _simple_vd(aed_parameter: DateParameterDescriptor) -> VersionDescriptor:
    return VersionDescriptor(True, [], aed_parameter, DateParameterDescriptor(False))


def test_assert_is_partition_matrix():
    with pytest.raises(ValueError):
        _assert_is_partition_matrix([])

    _assert_is_partition_matrix([_simple_vd(DateParameterDescriptor(False))])
    dt = date(2020, 1, 1)
    _assert_is_partition_matrix(
        [
            _simple_vd(DateParameterDescriptor(True, True)),
            _simple_vd(DateParameterDescriptor(True, False, None, dt)),
            _simple_vd(DateParameterDescriptor(True, False, dt, None)),
        ]
    )

    dt = date(2020, 1, 1)
    dt_ = date(2021, 1, 1)
    _assert_is_partition_matrix(
        [
            _simple_vd(DateParameterDescriptor(True, True)),
            _simple_vd(DateParameterDescriptor(True, False, None, dt)),
            _simple_vd(DateParameterDescriptor(True, False, dt, dt_)),
            _simple_vd(DateParameterDescriptor(True, False, dt_, None)),
        ]
    )
