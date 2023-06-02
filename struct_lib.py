from skyrim.falkreath import CLib1Tab1, CTable
from project_config import test_windows

database_structure: dict[str, CLib1Tab1] = {}

test_return_lbls = ["test_return_{:03d}".format(w) for w in test_windows]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {z: "REAL"},
        })
    ) for z in test_return_lbls})
