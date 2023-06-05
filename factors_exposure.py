import datetime as dt
import itertools as ittl
import multiprocessing as mp
from algs.factor_exposure_amt import fac_exp_alg_amt
from algs.factor_exposure_basis import fac_exp_alg_basis, fac_exp_alg_basis_ma_and_diff
from algs.factor_exposure_mtm import fac_exp_alg_mtm
from algs.factor_exposure_sgm import fac_exp_alg_sgm
from algs.factor_exposure_size import fac_exp_alg_size
from algs.factor_exposure_skew import fac_exp_alg_skew
from algs.factor_exposure_to import fac_exp_alg_to
from algs.factor_exposure_ts import fac_exp_alg_ts, fac_exp_alg_ts_ma_and_diff


def cal_fac_exp_amt_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str,
                       amt_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       money_scale: int):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in amt_windows:
        pool.apply_async(fac_exp_alg_amt,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir,
                               money_scale))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_basis_mp(proc_num: int,
                         run_mode: str, bgn_date: str, stp_date: str,
                         basis_windows: list[int],
                         instruments_universe: list[str],
                         database_structure: dict,
                         major_return_dir: str,
                         equity_index_by_instrument_dir: str,
                         factors_exposure_dir: str,
                         calendar_path: str):
    t0 = dt.datetime.now()
    fac_exp_alg_basis(run_mode, bgn_date, stp_date,
                      instruments_universe,
                      database_structure,
                      major_return_dir,
                      equity_index_by_instrument_dir,
                      factors_exposure_dir)
    pool = mp.Pool(processes=proc_num)
    for p_window in basis_windows:
        pool.apply_async(fac_exp_alg_basis_ma_and_diff,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_mtm_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str,
                       mtm_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window, tag_adj in ittl.product(mtm_windows, (False, True)):
        pool.apply_async(fac_exp_alg_mtm,
                         args=(run_mode, bgn_date, stp_date, p_window, tag_adj,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_sgm_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str,
                       sgm_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in sgm_windows:
        pool.apply_async(fac_exp_alg_sgm,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_size_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str,
                        size_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        major_return_dir: str,
                        factors_exposure_dir: str,
                        ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in size_windows:
        pool.apply_async(fac_exp_alg_size,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_skew_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str,
                        skew_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        major_return_dir: str,
                        factors_exposure_dir: str,
                        ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in skew_windows:
        pool.apply_async(fac_exp_alg_skew,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_to_mp(proc_num: int,
                      run_mode: str, bgn_date: str, stp_date: str,
                      to_windows: list[int],
                      instruments_universe: list[str],
                      database_structure: dict,
                      major_return_dir: str,
                      factors_exposure_dir: str,
                      ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in to_windows:
        pool.apply_async(fac_exp_alg_to,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_ts_mp(proc_num: int,
                      run_mode: str, bgn_date: str, stp_date: str,
                      ts_windows: list[int],
                      instruments_universe: list[str],
                      database_structure: dict,
                      major_minor_dir: str,
                      md_dir: str,
                      factors_exposure_dir: str,
                      calendar_path: str,
                      price_type: str):
    t0 = dt.datetime.now()
    fac_exp_alg_ts(run_mode, bgn_date, stp_date,
                   instruments_universe,
                   database_structure,
                   major_minor_dir,
                   md_dir,
                   factors_exposure_dir,
                   price_type)
    pool = mp.Pool(processes=proc_num)
    for p_window in ts_windows:
        pool.apply_async(fac_exp_alg_ts_ma_and_diff,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
