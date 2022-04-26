import datetime
import re

DELTA0 = datetime.timedelta(0)

rfc3339_datetime_re = re.compile(r'(\d\d\d\d)-(\d\d)-(\d\d)[ tT](\d\d):(\d\d):(\d\d)(\.(\d+))?([zZ]|(([-+])(\d\d):?(\d\d)))')


def _offset_to_tz_name(offset):
    offset = int(offset)
    if offset < 0:
        tz_sign = '-'
    else:
        tz_sign = '+'
    offset = abs(offset)
    tz_hour = offset / 60
    tz_min = offset % 60
    return '%s%02d:%02d' % (tz_sign, tz_hour, tz_min)


class TzInfo(datetime.tzinfo):

    def __init__(self, minutes_east=0, name='Z'):

        self.minutes_east = minutes_east
        self.offset = datetime.timedelta(minutes=minutes_east)
        self.name = name

    def utcoffset(self, dt):
        return self.offset

    def dst(self, dt):
        return DELTA0

    def tzname(self, dt):
        return self.name

    def __repr__(self):
        if self.minutes_east == 0:
            return "rfc3339.UTC_TZ"
        else:
            return "rfc3339.tzinfo(%s,%s)" % (self.minutes_east, repr(self.name))


UTC_TZ = TzInfo(0, 'Z')


def _parse_components(s, hour, minutes, sec, frac_sec, tz_whole, tz_sign, tz_hour, tz_min):
    if frac_sec:
        frac_sec = float('0.' + frac_sec)
    else:
        frac_sec = 0
    micro_sec = int((frac_sec * 1000000) + 0.5)

    if tz_whole == 'z' or tz_whole == 'Z':
        tz = UTC_TZ
    else:
        tz_hour = int(tz_hour)
        tz_min = int(tz_min)
        offset = tz_hour * 60 + tz_min
        if offset == 0:
            tz = UTC_TZ
        else:
            if tz_hour > 24 or tz_min > 60 or offset > 1439:
                raise ValueError('invalid offset', s, tz_whole)

            if tz_sign == '-':
                offset = -offset
            tz = TzInfo(offset, _offset_to_tz_name(offset))

    return int(hour), int(minutes), int(sec), micro_sec, tz


def parse(s):
    m = rfc3339_datetime_re.match(s)
    if m is None:
        raise ValueError('invalid datetime string', s)

    (y, m, d, hour, minutes, sec, ignore1, frac_sec, tz_whole, ignore2,
     tz_sign, tz_hour, tz_min) = m.groups()

    hour, minutes, sec, micro_sec, tz = _parse_components(
        s, hour, minutes, sec, frac_sec, tz_whole, tz_sign, tz_hour, tz_min)

    return datetime.datetime(
        int(y), int(m), int(d), hour, minutes, sec, micro_sec, tz)
