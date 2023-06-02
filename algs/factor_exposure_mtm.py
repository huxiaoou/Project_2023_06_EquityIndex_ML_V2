import os
import datetime as dt
import pandas as pd
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibWriter


def fac_exp_alg_mtm(
        run_mode: str, bgn_date: str, stp_date: str | None, mtm_window: int, tag_adj: bool,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        major_return_dir: str,
        factors_exposure_dir: str,
):
    factor_lbl = "MTM{:03d}ADJ".format(mtm_window) if tag_adj else "MTM{:03d}".format(mtm_window)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        major_return_file = "major_return.{}.close.csv.gz".format(instrument)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).set_index("trade_date")
        major_return_df[factor_lbl] = major_return_df["instru_idx"] / major_return_df["instru_idx"].shift(mtm_window) - 1
        if tag_adj:
            major_return_df["volatility"] = major_return_df["major_return"].rolling(mtm_window).std()
            major_return_df[factor_lbl] = major_return_df[factor_lbl] / major_return_df["volatility"] * (252 ** 0.5)
        fiter_dates = (major_return_df.index >= bgn_date) & (major_return_df.index < stp_date)
        factor_df = major_return_df.loc[fiter_dates, [factor_lbl]].copy()
        factor_df["instrument"] = instrument
        all_factor_dfs.append(factor_df[["instrument", factor_lbl]])

    # --- reorganize
    all_factor_df = pd.concat(all_factor_dfs, axis=0, ignore_index=False)
    all_factor_df.sort_index(inplace=True)

    # --- save
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibWriter(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
    factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
    factor_lib.close()

    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl))
    return 0
