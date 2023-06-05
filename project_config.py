import itertools as ittl

instruments_universe = ["IH.CFE", "IF.CFE", "IC.CFE"]
equity_indexes = (
    ("000016.SH", "IH.CFE"),
    ("000300.SH", "IF.CFE"),
    ("000905.SH", "IC.CFE"),
)

test_windows = [1, 2, 3, 5, 10]

# --- factors
factors_args = {
    "basis_windows": [21, 63, 126, 252],
    "ts_windows": [21, 63, 126, 252],
    "mtm_windows": [21, 63, 126, 252],

    "skew_windows": [21, 63, 126, 252],
    "amt_windows": [21, 63, 126, 252],
    "size_windows": [21, 63, 126, 252],
    "sgm_windows": [21, 63, 126, 252],
    "to_windows": [21, 63, 126, 252],

    "top": [0.1, 0.2, 0.5],
}
amt_windows = factors_args["amt_windows"]
basis_windows = factors_args["basis_windows"]
mtm_windows = factors_args["mtm_windows"]
sgm_windows = factors_args["sgm_windows"]
size_windows = factors_args["size_windows"]
skew_windows = factors_args["skew_windows"]
to_windows = factors_args["to_windows"]
ts_windows = factors_args["ts_windows"]

fac_sub_grp_amt = ["AMT{:03d}".format(_) for _ in amt_windows]
fac_sub_grp_basis = ["BASIS"] + ["BASIS_M{:03d}".format(_) for _ in basis_windows] + ["BASIS_D{:03d}".format(_) for _ in basis_windows]
fac_sub_grp_mtm = ["MTM{:03d}".format(_) for _ in mtm_windows] + ["MTM{:03d}ADJ".format(_) for _ in mtm_windows]
fac_sub_grp_sgm = ["SGM{:03d}".format(_) for _ in sgm_windows]
fac_sub_grp_size = ["SIZE{:03d}".format(_) for _ in size_windows]
fac_sub_grp_skew = ["SKEW{:03d}".format(_) for _ in skew_windows]
fac_sub_grp_to = ["TO{:03d}".format(_) for _ in to_windows]
fac_sub_grp_ts = ["TS"] + ["TS_M{:03d}".format(_) for _ in ts_windows] + ["TS_D{:03d}".format(_) for _ in ts_windows]

factors = fac_sub_grp_basis + fac_sub_grp_ts + fac_sub_grp_mtm \
          + fac_sub_grp_amt + fac_sub_grp_sgm + fac_sub_grp_skew + fac_sub_grp_size + fac_sub_grp_to

# --- simulation
cost_rate = 5e-4

if __name__ == "__main__":
    print("Number of factors = {}".format(len(factors)))
