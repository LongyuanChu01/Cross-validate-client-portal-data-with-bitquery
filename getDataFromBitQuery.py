# The Snowflake Connector library.
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd

import requests
import json
import datetime

import os


# network = bsc or ethereum
def getDataFromBitQuery(network, address):
    query = '''query ($network: EthereumNetwork!,
                              $address: String!,
                              $from: ISO8601DateTime,
                              $till: ISO8601DateTime){
                          ethereum(network: $network){
                            smartContractCalls( date: {since: $from till: $till },
                                smartContractAddress: {is: $address}) {
                                    external_calls: count(external: true)
                                    internal_calls: count(external: false)
                                    callers: count(uniq: callers)
                            }
                          }
                        }'''
    startTime = datetime.datetime.now() - datetime.timedelta(days=1)
    startTime = startTime.strftime("%Y-%m-%d")
    variables = {"network": network, "address": address, "from": startTime, "till": startTime, "dateFormat": "%Y-%m-%d"}

    url = 'https://graphql.bitquery.io'
    r = requests.post(url, json={'query': query, 'variables': variables})
    json_data = json.loads(r.text)
    df_data = json_data['data']['ethereum']['smartContractCalls'][0]
    # address, calls count, Uniq Callers
    res = [address, df_data['external_calls'] + df_data['internal_calls'], df_data['callers']]
    print(address)
    return res


def getDataFromSnowFlake(network):
    ctx = snow.connect(
        user=os.environ.get('SNOW_USER'),
        password=os.environ.get('SNOW_PASSWORD'),
        account=os.environ.get('SNOW_ACCOUNT')
    )

    cs = ctx.cursor()

    cs.execute("USE WAREHOUSE COMPUTE_WH;")
    cs.execute("USE DATABASE WATERDROP_DB;")

    try:
        if network == 'ethereum':
            results = cs.execute(
                "select * from PUBLIC.ETH_TO_ADDR_PRECOMPUTE where DATE=DATEADD(Day ,-1, current_date)  order by TXN_COUNTS desc limit 3;").fetchall()
        elif network == 'bsc':
            results = cs.execute(
                "select * from PUBLIC.BSC_TO_ADDR_PRECOMPUTE where DATE=DATEADD(Day ,-1, current_date)  order by TXN_COUNTS desc limit 3;").fetchall()
    finally:
        cs.close()
    ctx.close()

    snowFlake = pd.DataFrame(results)
    snowFlake.columns = ['DATE', 'TO_ADDRESS', 'PROJECT_LABEL', 'TXN_COUNTS', 'ACTIVE_ADDRESSES']

    temp = snowFlake['TO_ADDRESS'].str.split(":", expand=True)
    snowFlake['address'] = temp[1]

    print("Got data from SnowFlake")
    return snowFlake


def writeIntoSnowFlake(snowFlake, network):
    ctx = snow.connect(
        user=os.environ.get('SNOW_USER'),
        password=os.environ.get('SNOW_PASSWORD'),
        account=os.environ.get('SNOW_ACCOUNT')
    )

    cs = ctx.cursor()

    cs.execute("USE WAREHOUSE COMPUTE_WH;")
    cs.execute("USE DATABASE WATERDROP_DB;")
    cs.execute("USE SCHEMA TEST;")
    cur = ctx.cursor()
    try:
        if network == 'bsc':
            write_pandas(ctx, snowFlake, "TEST_BSC_FROM_BITQUERY")
        elif network == 'ethereum':
            write_pandas(ctx, snowFlake, "TEST_ETH_FROM_BITQUERY")
    finally:
        cur.close()
        cs.close()
    ctx.close()
    print("Wrote data to SnowFlake")


def main():
    # network can be "ethereum" or "bsc"
    networks = ["ethereum", "bsc"]
    for network in networks:
        snowFlake = getDataFromSnowFlake(network)

        snowFlake['temp'] = snowFlake['address'].apply(lambda x: getDataFromBitQuery(network, x))

        # new df from the column of lists
        split_df = pd.DataFrame(snowFlake['temp'].tolist(),
                                columns=['address', 'TXN_COUNTS_BITQUERY', 'ACTIVE_ADDRESSES_BITQUERY'])
        # concat df and split_df
        snowFlake = pd.concat([snowFlake, split_df], axis=1)

        snowFlake.drop(columns=['address', 'temp'], inplace=True)
        snowFlake = snowFlake[['DATE', 'TO_ADDRESS', 'TXN_COUNTS_BITQUERY', 'ACTIVE_ADDRESSES_BITQUERY']]

        writeIntoSnowFlake(snowFlake, network)


if __name__ == "__main__":
    main()
