import itertools as ittl

instruments_universe = ["IH.CFE", "IF.CFE", "IC.CFE"]
equity_indexes = (
    ("000016.SH", "IH.CFE"),
    ("000300.SH", "IF.CFE"),
    ("000905.SH", "IC.CFE"),
)

sqlite3_tables = {
    "features_and_return": {
        "table_name": "features_and_return",
        "primary_keys": {
            "trade_date": "TEXT",
            "instrument": "TEXT",
            "contract": "TEXT",
            "tid": "TEXT",
            "timestamp": "INT4",
        },
        "value_columns": {"value": "REAL"}
    },

}

# --- simulation
cost_rate = 5e-4

if __name__ == "__main__":
    pass
