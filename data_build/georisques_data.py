from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional


from envinorma.data import Nomenclature, Regime, RubriqueSimpleThresholds
from envinorma.data.installation import Seveso
from envinorma.utils import snake_to_camel


class GRClassementActivite(Enum):
    ACTIVE = '1'
    INACTIVE = '0'


class GRRegime(Enum):
    AUTORISATION = 'Autorisation'
    ENREGISTREMENT = 'Enregistrement'
    INCONNU = 'Inconnu'


class GRIdRegime(Enum):
    AUTORISATION = 'A'
    ENREGISTREMENT = 'E'
    INCONNU = 'NC'


class FamilleNomenclature(Enum):
    OLD = 'xxx'
    ONE = '1xxx'
    TWO = '2xxx'
    THREE = '3xxx'
    FOUR = '4xxx'


@dataclass
class GRClassement:
    seveso: Seveso
    code_nomenclature: str
    alinea: Optional[str]
    date_autorisation: Optional[date]
    etat_activite: Optional[GRClassementActivite]
    regime: Optional[GRRegime]
    id_regime: Optional[GRIdRegime]
    activite_nomenclature_inst: str
    famille_nomenclature: Optional[FamilleNomenclature]
    volume_inst: Optional[str]
    unite: Optional[str]
    theoretical_regime: Optional[Regime] = None

    @staticmethod
    def from_georisques_dict(dict_: Dict[str, Any]) -> 'GRClassement':
        dict_ = {snake_to_camel(key): value for key, value in dict_.items()}
        dict_['seveso'] = Seveso(dict_['seveso'])
        dict_['date_autorisation'] = (
            datetime.strptime(dict_['date_autorisation'], '%Y-%m-%d').date() if dict_['date_autorisation'] else None
        )
        dict_['etat_activite'] = GRClassementActivite(dict_['etat_activite']) if dict_['etat_activite'] else None
        dict_['regime'] = GRRegime(dict_['regime']) if dict_['regime'] else None
        dict_['id_regime'] = GRIdRegime(dict_['id_regime']) if dict_['id_regime'] else None
        dict_['famille_nomenclature'] = (
            FamilleNomenclature(dict_['famille_nomenclature']) if dict_['famille_nomenclature'] else None
        )
        return GRClassement(**dict_)

    def to_dict(self) -> Dict[str, Any]:
        res = asdict(self)
        res['seveso'] = self.seveso.value
        if self.date_autorisation:
            res['date_autorisation'] = self.date_autorisation.strftime('%Y-%m-%d')
        if self.etat_activite:
            res['etat_activite'] = self.etat_activite.value
        if self.regime:
            res['regime'] = self.regime.value
        if self.id_regime:
            res['id_regime'] = self.id_regime.value
        if self.famille_nomenclature:
            res['famille_nomenclature'] = self.famille_nomenclature.value
        return res


def _compute_regime(value: float, rubrique: RubriqueSimpleThresholds) -> Regime:
    for i, threshold in enumerate(rubrique.thresholds[::-1]):
        if value >= threshold:
            return rubrique.regimes[len(rubrique.regimes) - 1 - i]
    return Regime.NC


def _extract_float_value(volume_str: Optional[str]) -> float:
    if not volume_str:
        raise ValueError('Expecting volume to extract value.')
    try:
        return float(volume_str)
    except ValueError:
        raise ValueError(f'Unhandled value: {volume_str}')


def _deduce_regime(classement: GRClassement, rubrique: RubriqueSimpleThresholds) -> Optional[Regime]:
    if classement.volume_inst is None:
        return None
    value = _extract_float_value(classement.volume_inst)
    return _compute_regime(value, rubrique)


def _is_in_nomenclature(code: str, nomenclature: Nomenclature) -> bool:
    if code and code in nomenclature.simple_thresholds:
        return True
    return False


def deduce_regime_if_possible(classement: GRClassement, nomenclature: Nomenclature) -> Optional[Regime]:
    if _is_in_nomenclature(classement.code_nomenclature, nomenclature):
        return _deduce_regime(classement, nomenclature.simple_thresholds[classement.code_nomenclature])
    return None


def add_theoretical_regime(
    classements: Dict[str, List[GRClassement]], nomenclature: Nomenclature
) -> Dict[str, List[GRClassement]]:
    return {
        id_: [replace(cl, theoretical_regime=deduce_regime_if_possible(cl, nomenclature)) for cl in clss]
        for id_, clss in classements.items()
    }
