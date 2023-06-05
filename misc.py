import os
import pandas as pd


def split_spot_daily_k(equity_index_by_instrument_dir: str, equity_indexes: tuple[tuple]):
    daily_k_file = "daily_k.xlsx"
    daily_k_path = os.path.join(equity_index_by_instrument_dir, daily_k_file)
    for equity_index_code, _ in equity_indexes:
        daily_k_df = pd.read_excel(daily_k_path, sheet_name=equity_index_code)
        daily_k_df["trade_date"] = daily_k_df["trade_date"].map(lambda z: z.strftime("%Y%m%d"))
        equity_index_file = "{}.csv".format(equity_index_code)
        equity_index_path = os.path.join(equity_index_by_instrument_dir, equity_index_file)
        daily_k_df.to_csv(equity_index_path, index=False, float_format="%.4f")
        print("...", equity_index_code, "saved as csvs")
    return 0
