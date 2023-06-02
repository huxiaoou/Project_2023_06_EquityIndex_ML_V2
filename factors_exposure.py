from algs.factor_exposure_basis import fac_exp_alg_basis, fac_exp_alg_basis_ma_and_diff
from algs.factor_exposure_ts import fac_exp_alg_ts, fac_exp_alg_ts_ma_and_diff
import multiprocessing as mp
import datetime as dt


def cal_fac_exp_basis_mp(proc_num: int,
                         run_mode: str, bgn_date: str, stp_date: str,
                         basis_windows: list[int],
                         instruments_universe: list[str],
                         database_structure: dict,
                         major_return_dir: str,
                         equity_index_by_instrument_dir: str,
                         factors_exposure_dir: str,
                         calendar_path: str,
                         ):
    t0 = dt.datetime.now()
    fac_exp_alg_basis(
        run_mode, bgn_date, stp_date,
        instruments_universe,
        database_structure,
        major_return_dir,
        equity_index_by_instrument_dir,
        factors_exposure_dir
    )

    pool = mp.Pool(processes=proc_num)
    for basis_window in basis_windows:
        pool.apply_async(
            fac_exp_alg_basis_ma_and_diff,
            args=(run_mode, bgn_date, stp_date, basis_window,
                  instruments_universe,
                  calendar_path,
                  database_structure,
                  factors_exposure_dir,
                  ),
        )
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
                      price_type: str,
                      ):
    t0 = dt.datetime.now()
    fac_exp_alg_ts(
        run_mode, bgn_date, stp_date,
        instruments_universe,
        database_structure,
        major_minor_dir,
        md_dir,
        factors_exposure_dir,
        price_type,
    )

    pool = mp.Pool(processes=proc_num)
    for ts_window in ts_windows:
        pool.apply_async(
            fac_exp_alg_ts_ma_and_diff,
            args=(run_mode, bgn_date, stp_date, ts_window,
                  instruments_universe,
                  calendar_path,
                  database_structure,
                  factors_exposure_dir,
                  ),
        )
    pool.close()
    pool.join()

    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
