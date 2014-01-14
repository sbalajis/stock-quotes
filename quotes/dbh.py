import psycopg2

class dbh:
    def __init__(self, database, user=None, password=None, host=None):
        try:
            self.con = psycopg2.connect(database=database, user=user, password=password, host=host)
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e

    def tickers(self):
        cur = self.con.cursor()
        cur.execute("""
                    select c.id, c.ticker
                    from portfolio_companies as p, companies as c
                    where p.company_id = c.id
                    order by c.ticker
                    """)
        return cur.fetchall()

    def deactivate(self, id):
        cur = self.con.cursor()
        cur.execute("""
                    UPDATE companies
                    SET active = false
                    WHERE id = %s
                    """, [id])
        self.con.commit()

    def last_date(self, id):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT price_date
                    FROM prices
                    WHERE company_id = %s
                    ORDER BY price_date desc
                    """, [id])
        return cur.fetchone()

    def save(self, params):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT id
                    FROM prices
                    WHERE company_id = %s and price_date = %s
                    """, [params['company_id'], params['price_date']])
        res = cur.fetchone()
        # print res

        if not res:
            # print "Adding Price"
            cur.execute("""
                    INSERT INTO prices (company_id, price_date, open, high, low, close, volume, adj_close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, [params['company_id'], params['price_date'], params['open'], params['high'], params['low'], params['close'], params['volume'], params['adj_close']])
            self.con.commit()
