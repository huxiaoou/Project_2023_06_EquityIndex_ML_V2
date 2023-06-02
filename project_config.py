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
}
basis_windows = factors_args["basis_windows"]
ts_windows = factors_args["ts_windows"]
fac_sub_grp_basis = ["BASIS"] + ["BASIS_M{:03d}".format(_) for _ in basis_windows] + ["BASIS_D{:03d}".format(_) for _ in basis_windows]
fac_sub_grp_ts = ["TS"] + ["TS_M{:03d}".format(_) for _ in basis_windows] + ["TS_D{:03d}".format(_) for _ in basis_windows]
factors = fac_sub_grp_basis + fac_sub_grp_ts

# --- simulation
cost_rate = 5e-4

if __name__ == "__main__":
    print("Number of factors = {}".format(len(factors)))
