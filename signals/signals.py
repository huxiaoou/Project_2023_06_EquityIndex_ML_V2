import datetime as dt
import numpy as np
import pandas as pd
import multiprocessing as mp
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader, CManagerLibWriter
from skyrim.whiterun import CCalendar
from skyrim.riften import CNAV
from skyrim.winterhold import plot_lines


def cal_simu_ret_by_date(df: pd.DataFrame, fe: str, tr: str):
    return df[fe] @ df[tr]


def shift_wgt(df: pd.DataFrame, row: str, col: str, val: str, shift_win: int):
    _pivot_df = pd.pivot_table(data=df, index=row, columns=col, values=val)
    _res_df = _pivot_df.shift(shift_win).stack().sort_index().reset_index()
    _res_df.rename(mapper={0: val}, axis=1, inplace=True)
    _dlt_wgt_srs = (_pivot_df - _pivot_df.shift(shift_win - 1)).shift(1).abs().sum(axis=1)
    return _res_df, _dlt_wgt_srs


class CSignal(object):
    def __init__(self, t_sid: str, t_sig_struct: dict, t_universe_options: dict[str, list[str]]):
        self.m_sid = t_sid
        self.m_mov_ave_win = t_sig_struct["mov_ave_win"]
        self.m_uid = t_sig_struct["uid"]
        self.m_factors, self.m_weights = zip(*t_sig_struct["factors"])
        self.m_weights = pd.Series(data=self.m_weights / np.sum(self.m_weights), index=self.m_factors)
        self.m_universe = t_universe_options[self.m_uid]
        self.m_u_size = len(self.m_universe)
        k = int(self.m_u_size / 2)
        wh = np.array([1] * k + [0] * (self.m_u_size - 2 * k) + [-1] * k)
        self.m_wh = wh / np.abs(wh).sum()

    def convert(self, xs: pd.Series, d: dict):
        d[xs.name] = pd.Series(data=self.m_wh, index=xs.sort_values(ascending=False).index)
        return 0

    def sumprod_weights(self, x: pd.DataFrame):
        xt = x.set_index("factor").T
        return xt @ self.m_weights[xt.columns]

    def merge_signals(self, run_mode: str, bgn_date: str, stp_date: str,
                      database_structure: dict[str, CLib1Tab1],
                      factors_exposure_dir: str,
                      signals_dir: str,
                      calendar_path: str):
        # --- load calendar
        calendar = CCalendar(calendar_path)
        iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
        base_date = calendar.get_next_date(iter_dates[0], -self.m_mov_ave_win - 1)

        # --- convert factor pos
        fac_sig_raw_dfs = []
        for factor_lbl in self.m_factors:
            factor_lib_structure = database_structure[factor_lbl]
            factor_lib = CManagerLibReader(t_db_save_dir=factors_exposure_dir, t_db_name=factor_lib_structure.m_lib_name)
            factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
            fac_exp_df = factor_lib.read_by_conditions(t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
            ], t_value_columns=["trade_date", "instrument", "value"])
            factor_lib.close()
            exp_df_by_date = pd.pivot_table(data=fac_exp_df, values="value", index="trade_date", columns="instrument")
            res = {}
            exp_df_by_date[self.m_universe].apply(self.convert, axis=1, d=res)
            fac_sig_raw_df = pd.DataFrame.from_dict(res, orient="index")
            fac_sig_raw_df["factor"] = factor_lbl
            fac_sig_raw_dfs.append(fac_sig_raw_df)

        sig_raw_df = pd.concat(fac_sig_raw_dfs, axis=0, ignore_index=False)
        sig_grp_df = sig_raw_df[self.m_universe + ["factor"]].groupby(lambda z: z).apply(self.sumprod_weights)
        # sig_rol_df = sig_grp_df.rolling(window=self.m_mov_ave_win).mean()
        sig_nrm_df = sig_grp_df.div(sig_grp_df.abs().sum(axis=1), axis=0).fillna(0)
        sig_rol_df = sig_nrm_df.rolling(window=self.m_mov_ave_win).mean()

        sig_df = sig_rol_df.div(sig_rol_df.abs().sum(axis=1), axis=0).fillna(0)
        sig_sav_df = sig_df.stack().reset_index(level=1)

        # --- save to lib
        sig_lib_id = self.m_sid
        sig_lib_structure = database_structure[sig_lib_id]
        sig_lib = CManagerLibWriter(t_db_save_dir=signals_dir, t_db_name=sig_lib_structure.m_lib_name)
        sig_lib.initialize_table(t_table=sig_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
        sig_lib.update(sig_sav_df, t_using_index=True)
        sig_lib.close()
        return 0

    def simulation(self, cost_rate: float,
                   run_mode: str, bgn_date: str, stp_date: str,
                   database_structure: dict[str, CLib1Tab1],
                   signals_dir: str,
                   test_returns_dir: str,
                   simulations_dir: str,
                   calendar_path: str):
        fix_test_window = 1

        # --- load calendar
        calendar = CCalendar(calendar_path)
        iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
        base_date = calendar.get_next_date(iter_dates[0], -fix_test_window - 1)

        # --- signals
        sig_lib_id = self.m_sid
        sig_lib_structure = database_structure[sig_lib_id]
        sig_lib = CManagerLibReader(t_db_save_dir=signals_dir, t_db_name=sig_lib_structure.m_lib_name)
        sig_lib.set_default(sig_lib_structure.m_tab.m_table_name)
        sig_df = sig_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
            ], t_value_columns=["trade_date", "instrument", "value"]
        )
        sig_lib.close()
        sig_df_shift, dlt_wgt_srs = shift_wgt(sig_df, row="trade_date", col="instrument", val="value", shift_win=fix_test_window + 1)

        # --- test return library
        test_return_lib_id = "test_return_{:03d}".format(1)
        test_return_lib_structure = database_structure[test_return_lib_id]
        test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_returns_dir)
        test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)
        test_return_df = test_return_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", bgn_date),
                ("trade_date", "<", stp_date),
            ], t_value_columns=["trade_date", "instrument", "value"]
        )
        test_return_lib.close()

        simu_input_df = pd.merge(
            left=sig_df_shift, right=test_return_df,
            on=["trade_date", "instrument"], suffixes=("_e", "_r"),
            how="right"
        ).set_index("instrument").fillna(0)

        res_srs = simu_input_df.groupby(by="trade_date").apply(cal_simu_ret_by_date, fe="value_e", tr="value_r")
        simu_df = pd.DataFrame({
            "rawRet": res_srs,
            "dltWgt": dlt_wgt_srs,
        })
        simu_df["netRet"] = simu_df["rawRet"] - simu_df["dltWgt"] * cost_rate
        nav = CNAV(t_raw_nav_srs=simu_df["netRet"], t_annual_rf_rate=0, t_freq="D", t_type="RET")
        nav.cal_all_indicators()
        print("...", self.m_sid, "simulated",
              "mu  = {:.6f}".format(nav.m_return_mean / 100),
              "sd  = {:.6f}".format(nav.m_return_std / 100),
              "sr  = {:.3f}".format(nav.m_sharpe_ratio),
              "cr  = {:.3f}".format(nav.m_calmar_ratio),
              "mdd = {:.3f}".format(nav.m_max_drawdown_scale),)

        plot_lines(t_plot_df=simu_df[["rawRet", "netRet"]].cumsum(), t_fig_name="nav-" + self.m_sid, t_save_dir=simulations_dir)
        return 0


def cal_signals_mp(
        proc_num: int, sids: list[str],
        signals_structure: dict[str, dict], universe_options: dict[str, list[str]],
        run_mode: str, bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for sid in sids:
        sig_struct = signals_structure[sid]
        signal = CSignal(t_sid=sid, t_sig_struct=sig_struct, t_universe_options=universe_options)
        pool.apply_async(
            signal.merge_signals,
            args=(run_mode, bgn_date, stp_date),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_simulation_mp(
        proc_num: int, sids: list[str], cost_rate: float,
        signals_structure: dict[str, dict], universe_options: dict[str, list[str]],
        run_mode: str, bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for sid in sids:
        sig_struct = signals_structure[sid]
        signal = CSignal(t_sid=sid, t_sig_struct=sig_struct, t_universe_options=universe_options)
        pool.apply_async(
            signal.simulation,
            args=(cost_rate, run_mode, bgn_date, stp_date),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
