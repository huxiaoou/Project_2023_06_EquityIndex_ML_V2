"""
0.  Entry point of this project
1.  Suggested bgn_date for overwrite mode:
    {
        "update_major_minute": "20150416"
    }
"""
import argparse
from preprocess.preprocess import split_spot_daily_k, update_major_minute, update_public_info
from test_returns.test_returns import cal_test_returns_mp
from ic_tests.ic_tests import cal_ic_tests_mp
from ic_tests.ic_tests_summary import cal_ic_tests_summary
from project_config import equity_indexes, mapper_futures_to_index
from project_config import instruments_universe, test_windows
from project_config import factors, factors_args
from project_config import manager_cx_windows
from struct_lib import database_structure
from project_setup import calendar_path, futures_instru_info_path
from project_setup import major_minor_dir, major_return_dir, md_by_instru_dir
from project_setup import futures_md_dir, futures_md_structure_path
from project_setup import futures_md_db_name, futures_em01_db_name
from project_setup import futures_fundamental_intermediary_dir
from project_setup import equity_index_by_instrument_dir
from project_setup import research_test_returns_dir, research_factors_exposure_dir
from project_setup import research_intermediary_dir, research_ic_tests_dir, research_ic_tests_summary_dir
from factors_exposure import cal_fac_exp_amp_mp
from factors_exposure import cal_fac_exp_amt_mp
from factors_exposure import cal_fac_exp_basis_mp
from factors_exposure import cal_fac_exp_beta_mp
from factors_exposure import cal_fac_exp_cx_mp
from factors_exposure import cal_fac_exp_exr_mp
from factors_exposure import cal_fac_exp_mtm_mp
from factors_exposure import cal_fac_exp_pos_mp
from factors_exposure import cal_fac_exp_sgm_mp
from factors_exposure import cal_fac_exp_size_mp
from factors_exposure import cal_fac_exp_skew_mp
from factors_exposure import cal_fac_exp_smt_mp
from factors_exposure import cal_fac_exp_to_mp
from factors_exposure import cal_fac_exp_ts_mp
from factors_exposure import cal_fac_exp_twc_mp

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Entry point to run all")
    args_parser.add_argument("-s", "--switch", type=str, help="""
        use this to decide which parts to run, available options = {'preprocess', 'test_returns', 'factors_exposure'}
        """)
    args_parser.add_argument("-f", "--factor", type=str, default="", help="""
        optional, must be provided if switch = 'factors_exposure',
        use this to decide which factor, available options = {
        'amp', 'amt', 'basis', 'beta', 'cx', 'exr', 'mtm', 'pos', 'sgm', 'size', 'skew', 'smt', 'to', 'ts', 'twc'}
        """)
    args_parser.add_argument("-m", "--mode", type=str, default="", help="""
        run mode, available options = {'o', 'overwrite', 'a', 'append'}
        """)
    args_parser.add_argument("-p", "--process", type=int, default=5, help="""
        number of process to be called when calculating, default = 5
        """)
    args = args_parser.parse_args()
    switch = args.switch.upper()
    factor = args.factor.lower()
    run_mode = args.mode.upper()
    proc_num = args.process

    bgn_date, stp_date = "20160101", "20230529"
    m01_bgn_date, m01_stp_date = "20150416", "20230529"

    if switch in ["PP", "PREPROCESS"]:
        if factor == "split":
            split_spot_daily_k(equity_index_by_instrument_dir, equity_indexes)
        elif factor == "minute":
            update_major_minute(run_mode=run_mode, bgn_date=m01_bgn_date, stp_date=m01_stp_date,
                                instruments=instruments_universe, calendar_path=calendar_path,
                                futures_md_structure_path=futures_md_structure_path,
                                futures_em01_db_name=futures_em01_db_name,
                                futures_md_dir=futures_md_dir,
                                major_minor_dir=major_minor_dir,
                                intermediary_dir=research_intermediary_dir,
                                database_structure=database_structure)
        elif factor == "pub":
            update_public_info(value_type="pos",
                               run_mode=run_mode, bgn_date=m01_bgn_date, stp_date=m01_stp_date,
                               instruments=instruments_universe,
                               calendar_path=calendar_path,
                               futures_md_structure_path=futures_md_structure_path,
                               futures_md_db_name=futures_md_db_name,
                               futures_md_dir=futures_md_dir,
                               futures_fundamental_intermediary_dir=futures_fundamental_intermediary_dir,
                               intermediary_dir=research_intermediary_dir,
                               database_structure=database_structure
                               )
            update_public_info(value_type="delta",
                               run_mode=run_mode, bgn_date=m01_bgn_date, stp_date=m01_stp_date,
                               instruments=instruments_universe,
                               calendar_path=calendar_path,
                               futures_md_structure_path=futures_md_structure_path,
                               futures_md_db_name=futures_md_db_name,
                               futures_md_dir=futures_md_dir,
                               futures_fundamental_intermediary_dir=futures_fundamental_intermediary_dir,
                               intermediary_dir=research_intermediary_dir,
                               database_structure=database_structure
                               )
    elif switch in ["TR", "TEST_RETURNS"]:
        cal_test_returns_mp(
            proc_num=proc_num,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            test_windows=test_windows,
            instruments_universe=instruments_universe,
            database_structure=database_structure,
            test_returns_dir=research_test_returns_dir,
            major_minor_dir=major_minor_dir,
            futures_md_dir=futures_md_dir,
            calendar_path=calendar_path,
            futures_md_structure_path=futures_md_structure_path,
            futures_em01_db_name=futures_em01_db_name,
        )
    elif switch in ["FE", "FACTORS_EXPOSURE"]:
        if factor == "amp":
            cal_fac_exp_amp_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                amp_windows=factors_args["amp_windows"], lbds=factors_args["lbds"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                mapper_fut_to_idx=mapper_futures_to_index,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir
            )
        elif factor == "amt":
            cal_fac_exp_amt_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                amt_windows=factors_args["amt_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                money_scale=10000
            )
        elif factor == "basis":
            cal_fac_exp_basis_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                basis_windows=factors_args["basis_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
            )
        elif factor == "beta":
            cal_fac_exp_beta_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                beta_windows=factors_args["beta_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
            )
        elif factor == "cx":
            cal_fac_exp_cx_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                mgr_cx_windows=manager_cx_windows, top_props=factors_args["top_props"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "exr":
            cal_fac_exp_exr_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                exr_windows=factors_args["exr_windows"], drifts=factors_args["drifts"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
            )
        elif factor == "mtm":
            cal_fac_exp_mtm_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                mtm_windows=factors_args["mtm_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir,
            )
        elif factor == "pos":
            cal_fac_exp_pos_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                pos_windows=factors_args["pos_windows"], top_players_qty=factors_args["top_players_qty"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                test_returns_dir=research_test_returns_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
            )
        elif factor == "sgm":
            cal_fac_exp_sgm_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                sgm_windows=factors_args["sgm_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "size":
            cal_fac_exp_size_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                size_windows=factors_args["size_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "skew":
            cal_fac_exp_skew_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                skew_windows=factors_args["skew_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "smt":
            cal_fac_exp_smt_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                smt_windows=factors_args["smt_windows"], lbds=factors_args["lbds"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
                futures_instru_info_path=futures_instru_info_path,
                amount_scale=1e4
            )
        elif factor == "to":
            cal_fac_exp_to_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                to_windows=factors_args["to_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "ts":
            cal_fac_exp_ts_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                ts_windows=factors_args["ts_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_minor_dir=major_minor_dir,
                md_dir=md_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
                price_type="close",
            )
        elif factor == "twc":
            cal_fac_exp_twc_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                twc_windows=factors_args["twc_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
            )
        else:
            print("... factor = {} is not a legal option, please check again".format(factor))
    elif switch in ["IC"]:
        cal_ic_tests_mp(
            proc_num=5,
            factor_lbls=factors, test_windows=test_windows,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            ic_tests_dir=research_ic_tests_dir,
            factors_exposure_dir=research_factors_exposure_dir,
            test_returns_dir=research_test_returns_dir,
            database_structure=database_structure,
            calendar_path=calendar_path)
    elif switch in ["ICSUM"]:
        for test_window in test_windows:
            cal_ic_tests_summary(
                test_window=test_window, methods=["pearson"], plot_top_n=6,
                factors=factors,
                bgn_date=bgn_date, stp_date=stp_date,
                database_structure=database_structure,
                ic_tests_dir=research_ic_tests_dir,
                ic_tests_summary_dir=research_ic_tests_summary_dir,
                days_per_year=252)
    else:
        print("... switch = {} is not a legal option, please check again".format(switch))
