import os
import datetime as dt
import numpy as np
import pandas as pd
import multiprocessing as mp
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.winterhold import plot_lines


def cal_ic_tests_summary(
        test_window: int, factors: list[str],
        methods: list[str], plot_top_n: int,
        bgn_date: str, stp_date: str,
        database_structure: dict[str, CLib1Tab1],
        ic_tests_dir: str,
        ic_tests_summary_dir: str,
        days_per_year: int,
):
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_rows", 1000)
    method_tag = {"pearson": "p", "spearman": "s"}

    statistics_data = []
    ic_data = {_: {} for _ in methods}
    for factor_lbl in factors:
        ic_test_lib_id = "ic-{}-TW{:03d}".format(factor_lbl, test_window)
        ic_test_lib_structure = database_structure[ic_test_lib_id]
        ic_test_lib = CManagerLibReader(t_db_save_dir=os.path.join(ic_tests_dir, factor_lbl), t_db_name=ic_test_lib_structure.m_lib_name)
        ic_test_lib.set_default(ic_test_lib_structure.m_tab.m_table_name)

        ic_df = ic_test_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "pearson", "spearman", "CH", "CF", "FH"]).set_index("trade_date")
        ic_test_lib.close()
        obs = len(ic_df)
        res = {
            "factor": factor_lbl,
            "obs": obs,
            "propCH": ic_df["CH"].mean(),
            "propCF": ic_df["CF"].mean(),
            "propFH": ic_df["FH"].mean(),
        }
        for method in methods:
            ic_srs, mt = ic_df[method], method_tag[method]
            mu = ic_srs.mean()
            sd = ic_srs.std()
            icir = mu / sd * np.sqrt(days_per_year / test_window)
            prop_pos = sum(ic_srs > 0) / obs
            prop_neg = sum(ic_srs < 0) / obs
            res.update({
                mt + "ICMean": mu,
                mt + "ICStd": sd,
                mt + "ICIR": icir,
                mt + "ICPropPos": prop_pos,
                mt + "ICPropNeg": prop_neg,

            })
            ic_data[method][factor_lbl] = ic_srs
        statistics_data.append(res)

    sum_df = pd.DataFrame(statistics_data)
    sum_file = "ic_tests_summary-TW{:03d}.csv.gz".format(test_window)
    sum_path = os.path.join(ic_tests_summary_dir, sum_file)
    sum_df.to_csv(sum_path, index=False, float_format="%.6f")

    for method in methods:
        mt = method_tag[method]
        icir_tag = mt + "ICIR"
        sum_df.sort_values(by=icir_tag, ascending=False, inplace=True)
        factors_to_plot = sum_df.loc[sum_df[icir_tag].abs() >= 1.0, "factor"].tolist()
        if factors_to_plot:
            all_ic_df = pd.DataFrame(ic_data[method])
            all_ic_df_cumsum = all_ic_df.fillna(0).cumsum() / np.sqrt(test_window)
            plot_df = all_ic_df_cumsum[factors_to_plot]
            plot_lines(
                t_plot_df=plot_df, t_fig_name="ic_cumsum-TW{:03d}-{}".format(test_window, method),
                t_save_dir=ic_tests_summary_dir, t_colormap="jet",  # t_ylim=(-150, 90),
            )

            print("-" * 120)
            print("| test_window = {:>3d} | method = {:>12s} |".format(test_window, method))
            print(sum_df.head(plot_top_n))
            print(sum_df.tail(plot_top_n))
        else:
            print("... not enough factors are picked when method = {} with test_window = {}".format(method, test_window))
    return 0


def cal_ic_tests_summary_mp(
        proc_num: int,
        test_windows: list[int], factors: list[str],
        methods: list[str], plot_top_n: int,
        bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for test_window in test_windows:
        pool.apply_async(
            cal_ic_tests_summary,
            args=(test_window, factors, methods, plot_top_n, bgn_date, stp_date),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
