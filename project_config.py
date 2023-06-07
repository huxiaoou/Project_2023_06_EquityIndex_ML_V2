import itertools as ittl

instruments_universe = ["IH.CFE", "IF.CFE", "IC.CFE"]
equity_indexes = (
    ("000016.SH", "IH.CFE"),
    ("000300.SH", "IF.CFE"),
    ("000905.SH", "IC.CFE"),
    ("000852.SH", "IM.CFE"),
    ("881001.WI", None),
)
mapper_index_to_futures = {k: v for k, v in equity_indexes}
mapper_futures_to_index = {v: k for k, v in equity_indexes}

test_windows = [1, 2, 3, 5, 10]

# --- factors
factors_args = {
    "amt_windows": [21, 63, 126, 252],
    "amp_windows": [21, 63, 126, 252],
    "basis_windows": [21, 63, 126, 252],
    "beta_windows": [21, 63, 126, 252],
    "csp_windows": [21, 63, 126, 252],
    "csr_windows": [21, 63, 126, 252],
    "ctp_windows": [21, 63, 126, 252],
    "ctr_windows": [21, 63, 126, 252],
    "cvp_windows": [21, 63, 126, 252],
    "cvr_windows": [21, 63, 126, 252],
    "mtm_windows": [21, 63, 126, 252],
    "sgm_windows": [21, 63, 126, 252],
    "size_windows": [21, 63, 126, 252],
    "skew_windows": [21, 63, 126, 252],
    "smt_windows": [3, 5, 10, 21],
    "to_windows": [21, 63, 126, 252],
    "ts_windows": [21, 63, 126, 252],

    "top_props": [0.1, 0.2, 0.5, 1],
    "lbds": [0.2, 0.4, 0.6, 0.8],
}
amt_windows = factors_args["amt_windows"]
amp_windows = factors_args["amp_windows"]
basis_windows = factors_args["basis_windows"]
beta_windows = factors_args["beta_windows"]
csp_windows = factors_args["csp_windows"]
csr_windows = factors_args["csr_windows"]
ctp_windows = factors_args["ctp_windows"]
ctr_windows = factors_args["ctr_windows"]
cvp_windows = factors_args["cvp_windows"]
cvr_windows = factors_args["cvr_windows"]
mtm_windows = factors_args["mtm_windows"]
sgm_windows = factors_args["sgm_windows"]
size_windows = factors_args["size_windows"]
skew_windows = factors_args["skew_windows"]
smt_windows = factors_args["smt_windows"]
to_windows = factors_args["to_windows"]
ts_windows = factors_args["ts_windows"]
top_props = factors_args["top_props"]
lbds = factors_args["lbds"]

manager_cx_windows = {
    "CSP": csp_windows,
    "CSR": csr_windows,
    "CTP": ctp_windows,
    "CTR": ctr_windows,
    "CVP": cvp_windows,
    "CVR": cvr_windows,
}

fac_sub_grp_amt = ["AMT{:03d}".format(_) for _ in amt_windows]
fac_sub_grp_amp = ["AMPH{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(amp_windows, lbds)] \
                  + ["AMPL{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(amp_windows, lbds)] \
                  + ["AMPD{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(amp_windows, lbds)]
fac_sub_grp_basis = ["BASIS"] + ["BASIS_M{:03d}".format(_) for _ in basis_windows] + ["BASIS_D{:03d}".format(_) for _ in basis_windows]
fac_sub_grp_beta = ["BETA{:03d}".format(_) for _ in beta_windows] + ["BETA_D{:03d}".format(_) for _ in beta_windows[1:]]
fac_sub_grp_csp = ["CSP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(csp_windows, top_props)]
fac_sub_grp_csr = ["CSR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(csr_windows, top_props)]
fac_sub_grp_ctp = ["CTP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(ctp_windows, top_props)]
fac_sub_grp_ctr = ["CTR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(ctr_windows, top_props)]
fac_sub_grp_cvp = ["CVP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(cvp_windows, top_props)]
fac_sub_grp_cvr = ["CVR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(cvr_windows, top_props)]
fac_sub_grp_mtm = ["MTM{:03d}".format(_) for _ in mtm_windows] + ["MTM{:03d}ADJ".format(_) for _ in mtm_windows]
fac_sub_grp_sgm = ["SGM{:03d}".format(_) for _ in sgm_windows]
fac_sub_grp_size = ["SIZE{:03d}".format(_) for _ in size_windows]
fac_sub_grp_skew = ["SKEW{:03d}".format(_) for _ in skew_windows]
fac_sub_grp_smt = ["SMTP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(smt_windows, lbds)] + \
                  ["SMTR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(smt_windows, lbds)]
fac_sub_grp_to = ["TO{:03d}".format(_) for _ in to_windows]
fac_sub_grp_ts = ["TS"] + ["TS_M{:03d}".format(_) for _ in ts_windows] + ["TS_D{:03d}".format(_) for _ in ts_windows]

factors = fac_sub_grp_basis + fac_sub_grp_beta + fac_sub_grp_ts + fac_sub_grp_mtm \
          + fac_sub_grp_amt + fac_sub_grp_sgm + fac_sub_grp_skew + fac_sub_grp_size + fac_sub_grp_to \
          + fac_sub_grp_csp + fac_sub_grp_csr + fac_sub_grp_ctp + fac_sub_grp_ctr + fac_sub_grp_cvp + fac_sub_grp_cvr \
          + fac_sub_grp_amp + fac_sub_grp_smt

# --- simulation
cost_rate = 5e-4

if __name__ == "__main__":
    print("Number of factors = {}".format(len(factors)))
    print(mapper_index_to_futures)
    print(mapper_futures_to_index)
