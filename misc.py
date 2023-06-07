import os
import sys
import json
import datetime as dt
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CManagerLibReader, CTable
from skyrim.falkreath import CManagerLibWriter


def split_spot_daily_k(equity_index_by_instrument_dir: str, equity_indexes: tuple[tuple]):
    daily_k_file = "daily_k.xlsx"
    daily_k_path = os.path.join(equity_index_by_instrument_dir, daily_k_file)
    for equity_index_code, _ in equity_indexes:
        daily_k_df = pd.read_excel(daily_k_path, sheet_name=equity_index_code)
        daily_k_df["trade_date"] = daily_k_df["trade_date"].map(lambda z: z.strftime("%Y%m%d"))
        equity_index_file = "{}.csv".format(equity_index_code)
        equity_index_path = os.path.join(equity_index_by_instrument_dir, equity_index_file)
        daily_k_df.to_csv(equity_index_path, index=False, float_format="%.4f")
        print("...", equity_index_code, "saved as csv")
    return 0


def update_major_minute(
        run_mode: str, bgn_date: str, stp_date: str,
        instruments: list[str],
        calendar_path: str,
        futures_md_structure_path: str,
        futures_em01_db_name: str,
        futures_md_dir: str,
        major_minor_dir: str,
        intermediary_dir: str,
        database_structure: dict,
):
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- majors manager
    major_minor_manager = {}
    for instrument in instruments:
        major_minor_file = "major_minor.{}.csv.gz".format(instrument)
        major_minor_path = os.path.join(major_minor_dir, major_minor_file)
        major_minor_df = pd.read_csv(major_minor_path, dtype=str).set_index("trade_date")
        major_minor_manager[instrument] = major_minor_df

    # --- init lib reader
    with open(futures_md_structure_path, "r") as j:
        em01_table_struct = json.load(j)[futures_em01_db_name]["CTable"]
    em01_table = CTable(t_table_struct=em01_table_struct)
    em01_lib = CManagerLibReader(t_db_save_dir=futures_md_dir, t_db_name=futures_em01_db_name + ".db")
    em01_lib.set_default(em01_table.m_table_name)
    em01_cols = list(em01_table.m_primary_keys) + list(em01_table.m_value_columns)

    # --- init lib writer
    em01_major_lib_structure = database_structure["em01_major"]  # make sure it has the columns as em01_lib
    em01_major_lib = CManagerLibWriter(
        t_db_name=em01_major_lib_structure.m_lib_name,
        t_db_save_dir=intermediary_dir
    )
    em01_major_lib.initialize_table(t_table=em01_major_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- main loop
    dfs_list = []
    for trade_date in calendar.get_iter_list(bgn_date, stp_date, True):
        theory_number_of_bars = 270 if trade_date < "20160101" else 240
        for instrument in instruments:
            try:
                major_contract = major_minor_manager[instrument].at[trade_date, "n_contract"]
            except KeyError:
                print("... Error!", instrument, "does not have major contract @ ", trade_date)
                print("... called by misc.update_major_minute")
                sys.exit()
            major_contract_m01_df = em01_lib.read_by_conditions(
                t_conditions=[
                    ("trade_date", "=", trade_date),
                    ("loc_id", "=", major_contract),
                ], t_value_columns=em01_cols)
            if (num_of_bars := len(major_contract_m01_df)) != theory_number_of_bars:
                print("Error! Number of bars = {} @ {} for {} - {}, theory = {}".format(
                    num_of_bars, trade_date, instrument, major_contract, theory_number_of_bars))
                sys.exit()
            dfs_list.append(major_contract_m01_df)

    update_df = pd.concat(dfs_list, axis=0, ignore_index=True)
    em01_major_lib.update(t_update_df=update_df)

    em01_lib.close()
    em01_major_lib.close()
    return 0
