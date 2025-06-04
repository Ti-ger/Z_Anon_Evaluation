import pandas as pd
from re_identification_risk.csv2simple_auto import convert_csv2auto
from re_identification_risk.unicity_activites import risk_re_ident_quant
from datetime import timedelta
def quantifyer(log: pd.DataFrame, z: int, delta_t: timedelta, p_relative: list[float], p_absolute: list[int], repetition: int, projection: list[str] = ['A']):
    if len(projection) > 2 or len(projection) < 0:
        raise Exception("Projection list should not contain more than 2 projections or less than 1.")

    log = convert_csv2auto(log)

    return risk_re_ident_quant(z=z, delta_t=delta_t, df_data=log, points_relative=p_relative, points_absoulute=p_absolute, repetition=repetition, projection=projection)


