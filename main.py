import argparse
from test_returns import cal_test_returns_mp
from project_config import equity_indexes
from project_config import instruments_universe
from project_config import test_windows
from project_config import factors_args
from struct_lib import database_structure
from project_setup import calendar_path
from project_setup import major_minor_dir, major_return_dir, md_by_instru_dir
from project_setup import futures_md_structure_path
from project_setup import futures_em01_db_name
from project_setup import futures_md_dir
from project_setup import equity_index_by_instrument_dir
from project_setup import research_test_returns_dir
from project_setup import research_factors_exposure_dir
from misc import split_spot_daily_k
from factors_exposure import cal_fac_exp_amt_mp
from factors_exposure import cal_fac_exp_basis_mp
from factors_exposure import cal_fac_exp_beta_mp
from factors_exposure import cal_fac_exp_mtm_mp
from factors_exposure import cal_fac_exp_sgm_mp
from factors_exposure import cal_fac_exp_size_mp
from factors_exposure import cal_fac_exp_skew_mp
from factors_exposure import cal_fac_exp_to_mp
from factors_exposure import cal_fac_exp_ts_mp

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Entry point to run all")
    args_parser.add_argument("-s", "--switch", type=str, help="""
    use this to decide which parts to run, available options = ['test_returns', 'factors_exposure']
    """)
    args_parser.add_argument("-f", "--factor", type=str, default="", help="""
    optional, must be provided if switch = 'factors_exposure',
    use this to decide which factor, available options = ['basis']
    """)
    args = args_parser.parse_args()
    switch = args.switch.lower()
    factor = args.factor.lower()
    run_mode, bgn_date, stp_date = "o", "20160101", "20230529"

    if switch in ["pp", "preprocess"]:
        split_spot_daily_k(equity_index_by_instrument_dir, equity_indexes)
    elif switch in ["tr", "test_returns"]:
        cal_test_returns_mp(
            proc_num=5,
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
    elif switch in ["fe", "factors_exposure"]:
        if factor == "amt":
            cal_fac_exp_amt_mp(
                proc_num=5,
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
                proc_num=5,
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
                proc_num=5,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                beta_windows=factors_args["beta_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
            )
        elif factor == "mtm":
            cal_fac_exp_mtm_mp(
                proc_num=5,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                mtm_windows=factors_args["mtm_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir,
            )
        elif factor == "sgm":
            cal_fac_exp_sgm_mp(
                proc_num=5,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                sgm_windows=factors_args["sgm_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "size":
            cal_fac_exp_size_mp(
                proc_num=5,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                size_windows=factors_args["size_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "skew":
            cal_fac_exp_skew_mp(
                proc_num=5,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                skew_windows=factors_args["skew_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "to":
            cal_fac_exp_to_mp(
                proc_num=5,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                to_windows=factors_args["to_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "ts":
            cal_fac_exp_ts_mp(
                proc_num=5,
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
        else:
            print("... factor = {} is not a legal option, please check again".format(factor))
    else:
        print("... switch = {} is not a legal option, please check again".format(switch))
