from taosrest.timeutil import parse


def test_parse_time():
    s1 = '2018-10-03T14:38:15+08:00'
    t1 = parse(s1)
    assert t1.isoformat() == '2018-10-03T14:38:15+08:00'
    s2 = '2018-10-03T14:38:16.8+08:00'
    t2 = parse(s2)
    assert t2.isoformat() == '2018-10-03T14:38:16.800000+08:00'
    s3 = '2018-10-03T14:38:16.65+08:00'
    t3 = parse(s3)
    assert t3.isoformat() == '2018-10-03T14:38:16.650000+08:00'
