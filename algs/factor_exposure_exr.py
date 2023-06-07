import datetime as dt
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def find_extreme_return(t_df: pd.DataFrame, t_ret: str, t_drift: int):
    ret_min, ret_max, ret_median = t_df[t_ret].min(), t_df[t_ret].max(), t_df[t_ret].median()
    if (ret_max + ret_min) > (2 * ret_median):
        idx_exr, exr = t_df[t_ret].argmax(), ret_max
    else:
        idx_exr, exr = t_df[t_ret].argmin(), ret_min
    idx_dxr = idx_exr - t_drift
    if idx_dxr >= 0:
        dxr = t_df[t_ret].iloc[idx_dxr]
    else:
        dxr = exr  # use exr may result too many nans
    return exr, dxr


def fac_exp_alg_exr(
        run_mode: str, bgn_date: str, stp_date: str | None,
        exr_window: int, drift: int,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        intermediary_dir: str,
        calendar_path: str,
):
    factor_lbl = "EXR{:03d}D{:d}".format(exr_window, drift)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- init em01_major reader
    em01_major_lib_structure = database_structure["em01_major"]  # make sure it has the columns as em01_lib
    em01_major_lib = CManagerLibReader(
        t_db_name=em01_major_lib_structure.m_lib_name,
        t_db_save_dir=intermediary_dir
    )
    em01_major_lib.set_default(t_default_table_name=em01_major_lib_structure.m_tab.m_table_name)

    # ---
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -exr_window + 1)

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        em01_df = em01_major_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument.split(".")[0]),
            ], t_value_columns=["trade_date", "timestamp", "loc_id", "volume", "amount", "close", "preclose"]
        )
        em01_df[["close", "preclose"]] = em01_df[["close", "preclose"]].round(2)
        em01_df["m01_return_cls"] = (em01_df["close"] / em01_df["preclose"] - 1).replace(np.inf, 0)
        res_srs = em01_df.groupby(by="trade_date").apply(find_extreme_return, t_ret="m01_return_cls", t_drift=drift)
        exr_srs, dxr_srs = zip(*res_srs)
        exr_dxr_df = pd.DataFrame({
            "exr": exr_srs,
            "dxr": dxr_srs,
        }, index=res_srs.index).rolling(window=exr_window).mean()
        exr_dxr_df["instrument"] = instrument
        all_factor_dfs.append(exr_dxr_df)
    all_exr_dxr_df = pd.concat(all_factor_dfs, axis=0, ignore_index=False)
    exr_df = pd.pivot_table(data=all_exr_dxr_df, index="trade_date", columns="instrument", values="exr")
    dxr_df = pd.pivot_table(data=all_exr_dxr_df, index="trade_date", columns="instrument", values="dxr")
    exr_rank_df = exr_df.rank(axis=1)
    dxr_rank_df = dxr_df.rank(axis=1)
    rank_df = (exr_rank_df + dxr_rank_df) * 0.5
    rank_df = rank_df[rank_df.index >= bgn_date].sort_index(ascending=True)
    all_factor_df = rank_df.stack().reset_index(level=1)

    # --- save
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibWriter(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
    factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
    factor_lib.close()

    em01_major_lib.close()
    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl))
    return 0
