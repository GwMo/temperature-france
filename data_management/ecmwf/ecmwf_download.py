#!/usr/bin/env python
from calendar import monthrange
from datetime import datetime
import sys

from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer()

def parse_date(date):
    try:
        return datetime.strptime(date, '%Y-%m-%d')
    except ValueError as e:
        print('Could not parse date:')
        sys.exit('  ' + e.args[0])

def parse_dates(dates):
    start_date = parse_date(dates[0])
    end_date = parse_date(dates[1])

    if end_date < start_date:
        print('End date is before start date')
        temp = start_date; start_date = end_date; end_date = temp
        print('  Using ' + start_date.strftime('%Y-%m-%d') + ' as start and ' + end_date.strftime('%Y-%m-%d') + ' as end')

    return start_date, end_date

def retrieve_interim(start_date, end_date):
    print('Downloading ERA-INTERIM data from ' + start_date.strftime('%Y-%m-%d') + ' to ' + end_date.strftime('%Y-%m-%d'))
    print('  Data will be aggregated in monthly files')

    for year in range(start_date.year, end_date.year + 1):
        for month in range(1, 13):
            if year == start_date.year and month < start_date.month:
                continue # don't download data before start date
            elif year == start_date.year and month == start_date.month:
                start_string = start_date.strftime('%Y%m%d')
            else:
                start_string = '%04d%02d%02d' % (year, month, 1)

            if year == end_date.year and month > end_date.month:
                continue # don't download data after end date
            elif year == end_date.year and month == end_date.month:
                end_string = end_date.strftime('%Y%m%d')
            else:
                end_string = '%04d%02d%02d' % (year, month, monthrange(year, month)[1])

            request_dates = (start_string + "/TO/" + end_string)
            target = "era-interim_analysis_" + start_string + "_" + end_string + ".grib"
            interim_request(request_dates, target)

def interim_request(dates, target):
    print('    Downloading ' + dates + ' -> ' + target)
    server.retrieve({
        'class'   : "ei",          # ERA-INTERIM data
        'dataset' : "interim",     # ERA-INTERIM public dataset
        'stream'  : "oper",        # atmospheric high-resolution forecasting system
        'expver'  : "1",           # data version 1
        'type'    : "an",          # analysis fields
        'levtype' : "sfc",         # surface level
        'time'    : "00/06/12/18", # analysis at 0h, 6h, 12h, and 18h
        'step'    : "0",           # data from analysis base time
        'date'    : dates,
        'param'   : "31.128/32.128/33.128/34.128/35.128/39.128/134.128/136.128/137.128/139.128/141.128/151.128/164.128/165.128/166.128/167.128/168.128/173.128/174.128/198.128/206.128/234.128/235.128/238.128",
        'target'  : target,
    })

if __name__ == '__main__':
    dates = sys.argv[1:]
    if len(dates) != 2:
        print('Please specify a start date and an end date e.g. `python download.py 2000-01-01 2016-12-31`')
        sys.exit('  (Got ' + str(dates) + ')')
    else:
        start_date, end_date = parse_dates(dates)
        retrieve_interim(start_date, end_date)
