import argparse
from test_returns import cal_test_returns_mp
from project_config import instruments_universe
from project_config import test_windows
from project_config import factors_args
from struct_lib import database_structure
from project_setup import calendar_path
from project_setup import major_minor_dir, major_return_dir
from project_setup import futures_md_structure_path
from project_setup import futures_em01_db_name
from project_setup import futures_md_dir
from project_setup import equity_index_by_instrument_dir
from project_setup import research_test_returns_dir
from project_setup import research_factors_exposure_dir
from factors_exposure import cal_fac_exp_basis_mp

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

    if switch == "test_returns":
        cal_test_returns_mp(
            proc_num=5,
            run_mode="o", bgn_date="20160101", stp_date="20230529",
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

    elif switch == "factors_exposure":
        if factor == "basis":
            cal_fac_exp_basis_mp(
                proc_num=5,
                run_mode="o", bgn_date="20160101", stp_date="20230529",
                basis_windows=factors_args["basis_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                major_return_dir=major_return_dir,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir,
                factors_exposure_dir=research_factors_exposure_dir,
            )

    else:
        print("... switch = {} is not a legal option, please check again".format(switch))
