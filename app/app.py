from flask import Flask
from flask import request
# coding: utf-8

# In[1]:


import requests
import json
from typing import Dict, List
import pandas as pd


app = Flask(__name__)

# In[2]:


# Helper Methods for creating Dataframe object
default_columns = {
    "lot": {"lotSize1": -1},
    "address": {
        "country": "",
        "countrySubd": "",
        "line1": "",
        "line2": "",
        "locality": "",
        "matchCode": "",
        "oneLine": "",
        "postal1": "",
    },
    "location": {
        "accuracy": "",
        "elevation": -1,
        "latitude": "",
        "longitude": "",
        "distance": -1,
        "geoid": ""
    },
    "summary": {
        "yearbuilt": -1,
    },
    "building": {
        "size": {"universalsize": -1},
        "rooms": {"bathstotal": -1, "beds": -1}
    },
    "sale": {
        "salesearchdate": "",
        "saleTransDate": "",
        "amount": {
            "saleamt": -1,
            "salerecdate": "",
            "saledisclosuretype": -1,
            "saledocnum": "",
            "saletranstype": ""
        },
        "calculation": {"priceperbed": 0, "pricepersizeunit": 0}
    }
}


def getkeys(obj: Dict, stack: List[str]):
    for k, v in obj.items():
        k2 = [k] + stack  # don't return empty keys
        if v and isinstance(v, dict):
            for c in getkeys(v, k2):
                yield c
        else:  # leaf
            yield k2


def getvalues(obj):
    for k, v in obj.items():
        if not v and not isinstance(v, int):
            [0]
        if isinstance(v, dict):
            for c in getvalues(v):
                yield c
        else:  # leaf
            yield v if isinstance(v, list) else [v]


# In[3]:


# store json file to DataFrame object
def json_to_dataframe(properties):
    colunms = list(getkeys(default_columns, []))
    row = []
    for a in properties:
        values = []
        for column in colunms:
            column = column[::-1]
            try:
                val = a[column[0]]
                for i in range(1, len(column)):
                    val = val[column[i]]
                values.append(val)
            except KeyError:
                val = default_columns[column[0]]
                for i in range(1, len(column)):
                    val = val[column[i]]
                values.append(val)

        row.append(values)
    df = pd.DataFrame(row, columns=list(
        map(lambda l: "/".join(l[::-1]), list(getkeys(default_columns, [])))))
    return df


# Store api call to local json file
def api_call(zipcode, city, radius):
    properties = []
    for i in range(5):
        url = 'https://api.gateway.attomdata.com/propertyapi/v1.0.0/sale/snapshot'

        headers = {'Accept': 'application/json',
                   'apiKey': '2b57389fcd206f6e1107f4bab01213b4'}
        payload = {
            'address1': city,
            'address2': zipcode,
            'radius': radius,
            'minsaleamt': '1',
            'maxsaleamt': '1000000',
            'pagesize': '1000',
            'propertytype': 'RESIDENTIAL (NEC)',
            'page': i
        }
        data = requests.get(url, params=payload, headers=headers)
        try:
            res = data.json()["property"]
            properties.extend(res)
        except KeyError:
            break
    print(len(properties))
#     with open('./properties.json', 'w') as outfile:
#         json.dump(properties, outfile)
    return json_to_dataframe(properties)


# In[4]:


def get_paragraph_res(payment, low, med):
    if payment < low:
        return "You have a great payment! It will be hard to find cheaper housing in your area"
    elif payment < med:
        return "You have an okay payment, with some effort you can find cheaper housing"
    else:
        return "You have an average or above average payment, you can easily find cheaper housing in your area"


def get_tags(payment, low, med):
    if payment < low:
        return "Best Deal"
    elif payment < med:
        return "Great Deal"
    else:
        return "Bad Deal"


def get_cheap_nearby_locations(low, df):
    loc_ans = []
    df1 = df[(df['sale/amount/saleamt'] < low)]
    i = 0
    for index, row in df1.iterrows():
        if i > 4:
            break
        b = {}
        b['address'] = row['address/oneLine']
        beds = row['building/rooms/beds']
        bath = row['building/rooms/bathstotal']
        if beds == 0:
            beds = 2
        if int(bath) < 1.1:
            bath = 1
        b['beds'] = beds
        b['baths'] = bath
        b['amount'] = row['sale/amount/saleamt']
        loc_ans.append(b)
        i += 1
    return loc_ans
# priceComparison(zipcode, payment, beds?, bath?, radius?) : (low, mean. High, String)
# def priceComparison(zipcode, payment, beds, bath, current_payment, radius):


def priceComparison(zipcode, radius, cur_beds, cur_baths, current_payment):
    ans = {}
    bleh = requests.get(
        f"https://www.zipcodeapi.com/rest/EWxydk95XP16TvGogrB222ZL6QSFmoG4SPSscUxWJN77bPNrCIONUAOP6R4BnH7W/info.json/{zipcode}/degrees")
    city = bleh.json()["city"]
    df = api_call(zipcode, city, radius)
    df_filtered = df.filter(items=['address/line1', 'address/line2',
                                   'sale/amount/saleamt', 'sale/calculation/pricepersizeunit'])
    df1 = df_filtered[(df_filtered['sale/amount/saleamt'] > 50000)
                      & (df_filtered['sale/amount/saleamt'] < 1000000)]
    std = df1['sale/amount/saleamt'].std()
    ans['low'] = df1['sale/amount/saleamt'].quantile(0.05)
    ans['med'] = df1['sale/amount/saleamt'].quantile(0.11)
    ans['high'] = df1['sale/amount/saleamt'].quantile(0.18)
    ans['paragraph'] = get_paragraph_res(
        current_payment, ans['low'], ans['med'])
    ans['locations'] = get_cheap_nearby_locations(ans['med'], df)
    ans['tag'] = get_tags(current_payment, ans['low'], ans['med'])
    ans['current_payment'] = current_payment
    return ans


@app.route("/", methods=["POST"])
def hello():
    req_data = request.get_json()
    city = str(req_data["city"])
    zipcode = str(req_data["zipcode"])
    current_payment = int(req_data["current_payment"])
    cur_beds = int(req_data["beds"])
    cur_baths = int(req_data["baths"])
    radi = int(req_data["radius"])
    return str(priceComparison(zipcode, radi, cur_beds, cur_baths, current_payment))


@app.route("/test")
def test():
    return "test"


if __name__ == "__main__":
    app.run(debug=False)
