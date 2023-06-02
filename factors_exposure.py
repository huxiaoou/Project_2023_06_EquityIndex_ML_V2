from algs.factor_exposure_basis import fac_exp_alg_basis
import multiprocessing as mp
import datetime as dt


def cal_fac_exp_basis_mp(proc_num: int,
                         run_mode: str, bgn_date: str, stp_date: str,
                         basis_windows: list[int],
                         **kwargs,
                         ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    pool.apply_async(fac_exp_alg_basis, args=(run_mode, bgn_date, stp_date), kwds=kwargs)
    for basis_window in basis_windows:
        print(basis_window)
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
