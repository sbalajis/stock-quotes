#!/usr/bin/env python

import sys
sys.path.append('.')
sys.path.append('..')
import quotes
from datetime import date
import ystockquote
import argparse
import time
import random

parser = argparse.ArgumentParser(description='Connection Information')
parser.add_argument('--database', dest='database', type=str, default='adaptive_development')
parser.add_argument('--user', dest='user', type=str, default='adaptive')
parser.add_argument('--password', dest='password', type=str)
parser.add_argument('--host', dest='host', type=str, default='localhost')
args = parser.parse_args()

db = quotes.dbh(args.database, user=args.user, password=args.password, host=args.host)

tickers = db.tickers()
current_date = date.today().isoformat()
print ystockquote.__version__

for ticker in tickers:
    # print "Download for %s (%s)" % (ticker[1], ticker[0])
    last_date = ''
    try:
        ld = db.last_date(ticker[0])
        last_date = ld[0].isoformat()
    except:
        last_date = '2000-01-01'

    print "%s from %s to %s" % (ticker[1], last_date, current_date)
    try:
        res = ystockquote.get_historical_prices(ticker[1], last_date, current_date)

        for d in iter(res):
            r = res[d]
            params = {
                'company_id': ticker[0],
                'price_date': d,
                'volume':     r['Volume'],
                'adj_close':  r['Adj Close'],
                'high':       r['High'],
                'low':        r['Low'],
                'close':      r['Close'],
                'open':       r['Open']
            }
            #print params
            db.save(params)
            #print ""
    except:
        db.deactivate(ticker[0])
        print "Error on %s" % ticker[1]

    time.sleep(random.randint(5,10))