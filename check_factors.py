import pandas as pd
from skyrim.falkreath import CManagerLibReader
from struct_lib import database_structure
from project_config import factors
from project_setup import research_factors_exposure_dir

sum_data = []
for factor in factors:
    factor_lib_structure = database_structure[factor]
    factor_lib = CManagerLibReader(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=research_factors_exposure_dir
    )
    factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
    factor_df = factor_lib.read(t_value_columns=["trade_date", "instrument", "value"])
    factor_lib.close()

    df = pd.pivot_table(data=factor_df, values="value", index="trade_date", columns="instrument")
    df.dropna(axis=0, how="any", inplace=True)
    sum_data.append({
        "factor": factor,
        "obs": len(df),
        "bgn": df.index[0],
        "stp": df.index[-1],
    })

sum_df = pd.DataFrame(sum_data)
sum_df.to_csv(".\\.tmp\\check_factor.csv", index=False)
print(sum_df)
