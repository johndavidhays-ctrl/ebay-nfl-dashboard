import urllib.parse
from app.db import init_db
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search
from app.db import get_conn

def sold_url(title):
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"

def run():
    init_db()
    token = get_app_token()

    queries = [
        '("NFL" OR football) (PSA OR BGS OR SGC) (rookie OR prizm OR optic)'
    ]

    for q in queries:
        data = browse_search(token, q)
        for item in data.get("itemSummaries", []):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                      INSERT INTO deals
                      (item_id,title,item_url,sold_url,buy_price,buy_shipping)
                      VALUES (%s,%s,%s,%s,%s,%s)
                      ON CONFLICT (item_id) DO NOTHING
                    """, (
                        item["itemId"],
                        item["title"],
                        item["itemWebUrl"],
                        sold_url(item["title"]),
                        float(item["price"]["value"]),
                        0
                    ))
                    conn.commit()

if __name__ == "__main__":
    run()
