# encoding:UTF-8
from dotenv import load_dotenv

from taos.cinterface import *
import taos

load_dotenv()


def test_is_null_type_value():
    if not taos.IS_V3:
        return
    #
    assert taos.bind2.IS_NULL_TYPE_FALSE == 0
    assert taos.bind2.IS_NULL_TYPE_TRUE == 1
    assert taos.bind2.IS_NULL_TYPE_IGNORE == 2
    print("pass test_is_null_type_value")


def test_ignore_class_inst():
    if not taos.IS_V3:
        return
    #
    assert taos.bind2.IGNORE == taos.IGNORE
    assert taos.bind2.IgnoreUpdateType(None) != taos.bind2.IgnoreUpdateType("None")
    print("pass test_ignore_class_inst")


def test_is_null_type_func():
    if not taos.IS_V3:
        return
    #
    assert taos.bind2.get_is_null_type(1) == 0
    assert taos.bind2.get_is_null_type(None) == 1
    assert taos.bind2.get_is_null_type(taos.IGNORE) == 2
    print("pass test_is_null_type_func")


def test_datetime_to_timestamp():
    if not taos.IS_V3:
        return
    #
    from datetime import datetime
    dt_str = "2020-01-01 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    seconds_diff = 1577836800
    # datetime
    assert taos.bind2._datetime_to_timestamp(dt, taos.PrecisionEnum.Milliseconds, 2) == taos.FieldType.C_BIGINT_NULL
    assert taos.bind2._datetime_to_timestamp(dt, taos.PrecisionEnum.Milliseconds) == seconds_diff * 1000
    assert taos.bind2._datetime_to_timestamp(dt, taos.PrecisionEnum.Microseconds) == seconds_diff * 1000000
    try:
        taos.bind2._datetime_to_timestamp(dt, 9)
        assert 1 == 2
    except:
        pass
    # float
    assert taos.bind2._datetime_to_timestamp(seconds_diff * 1.0, taos.PrecisionEnum.Milliseconds) == seconds_diff * 1000
    assert taos.bind2._datetime_to_timestamp(seconds_diff * 1.0, taos.PrecisionEnum.Microseconds) == seconds_diff * 1000000
    try:
        taos.bind2._datetime_to_timestamp(seconds_diff * 1.0, 9)
        assert 1 == 2
    except:
        pass
    # int
    assert taos.bind2._datetime_to_timestamp(seconds_diff, taos.PrecisionEnum.Milliseconds).value == ctypes.c_int64(seconds_diff).value
    # str
    assert taos.bind2._datetime_to_timestamp(dt_str, taos.PrecisionEnum.Milliseconds) == seconds_diff * 1000
    assert taos.bind2._datetime_to_timestamp(dt_str, taos.PrecisionEnum.Microseconds) == seconds_diff * 1000000
    try:
        taos.bind2._datetime_to_timestamp(dt_str, 9)
        assert 1 == 2
    except:
        pass
    # c_int64
    assert taos.bind2._datetime_to_timestamp(ctypes.c_int64(seconds_diff), taos.PrecisionEnum.Milliseconds).value == ctypes.c_int64(seconds_diff).value
    # other
    assert taos.bind2._datetime_to_timestamp(list(), taos.PrecisionEnum.Milliseconds) == FieldType.C_BIGINT_NULL
    print("pass test_datetime_to_timestamp")


def test_new_stmt2_binds():
    if not taos.IS_V3:
        return
    #
    size = 1
    binds = taos.new_stmt2_binds(size)
    assert binds is not None
    bind = binds[0]
    bind.set_value(FieldType.C_BOOL, [False, True, None])
    bind.set_value(FieldType.C_TINYINT, [1, -1, None])
    bind.set_value(FieldType.C_SMALLINT, [2, -2, None])
    bind.set_value(FieldType.C_INT, [3, -3, None])
    bind.set_value(FieldType.C_BIGINT, [4, -4, None])
    bind.set_value(FieldType.C_FLOAT, [5.5555, -5.5555, None])
    bind.set_value(FieldType.C_DOUBLE, [6.66666666, -6.66666666, None])
    bind.set_value(FieldType.C_VARCHAR, ["涛思数据", None, "a long string with 中文字符"])
    bind.set_value(FieldType.C_BINARY, ["涛思数据", None, "a long string with 中文字符"])
    bind.set_value(FieldType.C_NCHAR, ["涛思数据", None, "a long string with 中文字符"])
    bind.set_value(FieldType.C_JSON, ["{'hello': 'world'}"])
    bind.set_value(FieldType.C_VARBINARY, [[0x01, 0x02, 0x03, 0x04], [0x01, 0x02, 0x03, 0x04], [0x01, 0x02, 0x03, 0x04]])
    bind.set_value(FieldType.C_GEOMETRY, [
        bytearray([0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40]),
        bytearray([0x01, 00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40]),
        bytearray([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40]),
    ])
    bind.set_value(FieldType.C_TIMESTAMP, [1626861392589, 1626861392590, 1626861392591])
    bind.set_value(FieldType.C_TINYINT_UNSIGNED, [1, 100, None])
    bind.set_value(FieldType.C_SMALLINT_UNSIGNED, [2, 200, None])
    bind.set_value(FieldType.C_INT_UNSIGNED, [3, 300, None])
    bind.set_value(FieldType.C_BIGINT_UNSIGNED, [4, 400, None])
    print("pass test_new_stmt2_binds")


def test_new_bindv():
    if not taos.IS_V3:
        return
    #
    # prepare data
    tbanmes = ["d1", "d2", "d3"]
    tags = [
        ["grade1", 1],
        ["grade1", 2],
        ["grade1", 3]
    ]
    datas = [
        # class 1
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99]
        ],
            # class 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2"],
            [0, 1, 1, 0, 1],
            [298, 280, 260, 2100, 299]
        ],
            # class 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3"],
            [0, 1, 1, 0, 1],
            [398, 380, 360, 3100, 399]

        ]
    ]

    cnt_tbls = 3
    cnt_tags = 2
    cnt_cols = 4
    cnt_rows = 5

    # tags
    stmt2_tags = []
    for tag_list in tags:
        n = len(tag_list)
        assert n == cnt_tags
        binds: Array[taos.TaosStmt2Bind] = taos.new_stmt2_binds(n)
        binds[0].binary(tag_list[0])
        binds[1].int(tag_list[1])
        stmt2_tags.append(binds)
    #

    # cols
    stmt2_cols = []
    for data_list in datas:
        n = len(data_list)
        assert n == cnt_cols
        binds: Array[taos.TaosStmt2Bind] = taos.new_stmt2_binds(n)
        binds[0].timestamp(data_list[0])
        binds[1].binary(data_list[1])
        binds[2].bool(data_list[2])
        binds[3].int(data_list[3])
        stmt2_cols.append(binds)
    #

    bindv = taos.new_bindv(cnt_tbls, tbanmes, stmt2_tags, stmt2_cols)
    assert bindv is not None
    print("pass test_new_bindv")
