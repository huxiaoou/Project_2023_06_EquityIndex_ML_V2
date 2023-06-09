import os
import numpy as np
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.winterhold import plot_lines


def cal_ic_tests_summary(
        test_window: int, methods: list[str],
        factors: list[str],
        bgn_date: str, stp_date: str,
        plot_top_n: int,
        database_structure: dict[str, CLib1Tab1],
        ic_tests_dir: str,
        ic_tests_summary_dir: str,
        days_per_year: int,
):
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_rows", 1000)

    statistics_data = {_: [] for _ in methods}
    ic_data = {_: {} for _ in methods}
    for factor_lbl in factors:
        ic_test_lib_id = "ic-{}-TW{:03d}".format(factor_lbl, test_window)
        ic_test_lib_structure = database_structure[ic_test_lib_id]
        ic_test_lib = CManagerLibReader(t_db_save_dir=os.path.join(ic_tests_dir, factor_lbl), t_db_name=ic_test_lib_structure.m_lib_name)
        ic_test_lib.set_default(ic_test_lib_structure.m_tab.m_table_name)

        ic_df = ic_test_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "pearson", "spearman", "CH"]).set_index("trade_date")
        ic_test_lib.close()

        obs = len(ic_df)
        for method in methods:
            ic_srs = ic_df[method]
            ic_data[method][factor_lbl] = ic_srs
            mu = ic_srs.mean()
            sd = ic_srs.std()
            icir = mu / sd * np.sqrt(days_per_year / test_window)
            prop_pos = sum(ic_srs > 0) / obs
            prop_neg = sum(ic_srs < 0) / obs
            prop_ch = ic_df["CH"].mean()
            statistics_data[method].append({
                "factor": factor_lbl,
                "obs": obs,
                "ICMean": mu,
                "ICStd": sd,
                "ICIR": icir,
                "propPos": prop_pos,
                "propNeg": prop_neg,
                "propCH": prop_ch
            })

    for method, method_data in statistics_data.items():
        sum_df = pd.DataFrame(method_data).sort_values("propCH", ascending=False)
        sum_file = "ic_tests_summary-TW{:03d}-{}.csv.gz".format(test_window, method)
        sum_path = os.path.join(ic_tests_summary_dir, sum_file)
        sum_df.to_csv(sum_path, index=False, float_format="%.4f")

        all_ic_df = pd.DataFrame(ic_data[method])
        all_ic_df_cumsum = all_ic_df.fillna(0).cumsum() / np.sqrt(test_window)
        factors_to_plot = sum_df["factor"].head(plot_top_n).tolist() + sum_df["factor"].tail(plot_top_n).tolist()
        plot_df = all_ic_df_cumsum[factors_to_plot]
        plot_lines(
            t_plot_df=plot_df, t_fig_name="ic_cumsum-TW{:03d}-{}".format(test_window, method),
            t_save_dir=ic_tests_summary_dir, t_colormap="jet", t_ylim=(-150, 90),
        )

        print("-" * 120)
        print("| test_window = {:>3d} | method = {:>12s} |".format(test_window, method))
        print(sum_df.head(plot_top_n))
        print(sum_df.tail(plot_top_n))
    return 0
