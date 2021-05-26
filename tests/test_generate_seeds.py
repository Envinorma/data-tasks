from datetime import date
from envinorma.data import AMApplicability, UsedDateParameter
import pytest
import math

from data_build.validate.check_am import _is_a_partition, _assert_is_partition_matrix


def test_is_partition():
    assert _is_a_partition([(-math.inf, math.inf)])
    assert _is_a_partition([(-math.inf, 1), (1, math.inf)])
    assert _is_a_partition([(-math.inf, 1), (1, 10), (10, math.inf)])
    assert _is_a_partition([(-math.inf, 1), (1, 10), (10, 20), (20, math.inf)])
    assert _is_a_partition([(20, math.inf), (-math.inf, 1), (1, 10), (10, 20)])
    assert not _is_a_partition([(-math.inf, 1), (1, 20), (10, 20), (20, math.inf)])
    assert not _is_a_partition([(-math.inf, 1), (1, 10), (10, 20), (20, 30)])
    assert not _is_a_partition([])


def _simple_applicability(aed_parameter: UsedDateParameter) -> AMApplicability:
    return AMApplicability(True, [], aed_parameter, UsedDateParameter(False))


def test_assert_is_partition_matrix():
    with pytest.raises(ValueError):
        _assert_is_partition_matrix([])

    _assert_is_partition_matrix([_simple_applicability(UsedDateParameter(False))])
    dt = date(2020, 1, 1)
    _assert_is_partition_matrix(
        [
            _simple_applicability(UsedDateParameter(True, False)),
            _simple_applicability(UsedDateParameter(True, True, None, dt)),
            _simple_applicability(UsedDateParameter(True, True, dt, None)),
        ]
    )

    dt = date(2020, 1, 1)
    dt_ = date(2021, 1, 1)
    _assert_is_partition_matrix(
        [
            _simple_applicability(UsedDateParameter(True, False)),
            _simple_applicability(UsedDateParameter(True, True, None, dt)),
            _simple_applicability(UsedDateParameter(True, True, dt, dt_)),
            _simple_applicability(UsedDateParameter(True, True, dt_, None)),
        ]
    )
