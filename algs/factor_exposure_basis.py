import os
import datetime as dt
import pandas as pd
from skyrim.falkreath import CManagerLibWriterByDate, CLib1Tab1


def fac_exp_alg_basis(
        run_mode: str, bgn_date: str, stp_date: str,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        major_return_dir: str,
        equity_index_by_instrument_dir: str,
        factors_exposure_dir: str,
):
    factor_lbl = "BASIS"
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    equity_index_code_mapper = {
        "IH.CFE": "000016.SH",
        "IF.CFE": "000300.SH",
        "IC.CFE": "000905.SH",
        "IM.CFE": "000852.SH",
    }

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        major_return_file = "major_minor.{}.csv.gz".format(instrument)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype=str).set_index("trade_date")
        fiter_dates = (major_return_df.index >= bgn_date) & (major_return_df.index < stp_date)
        selected_major_return_df = major_return_df.loc[fiter_dates, ["n_contract", "close"]]

        equity_index_code = equity_index_code_mapper[instrument]
        spot_data_file = "{}.csv".format(equity_index_code)
        spot_data_path = os.path.join(equity_index_by_instrument_dir, spot_data_file)
        spot_df = pd.read_csv(spot_data_path, dtype={"trade_date": str}).set_index("trade_date")
        fiter_dates = (spot_df.index >= bgn_date) & (spot_df.index < stp_date)
        selected_spot_df = spot_df.loc[fiter_dates, ["close"]]

        factor_df = pd.merge(
            left=selected_major_return_df, right=selected_spot_df,
            left_index=True, right_index=True, how="outer",
            suffixes=("_f", "_s")
        )
        factor_df["instrument"] = instrument
        factor_df[factor_lbl] = factor_df["close_f"] / factor_df["close_s"] - 1
        all_factor_dfs.append(factor_df[["instrument", factor_lbl]])

    # --- reorganize
    all_factor_df = pd.concat(all_factor_dfs, axis=0, ignore_index=False)

    # --- save
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibWriterByDate(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
    factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
    factor_lib.close()

    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl))
    return 0
