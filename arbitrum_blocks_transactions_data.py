import pandas as pd
from web3 import Web3, HTTPProvider
from clickhouse_driver import Client
import warnings

from web3.exceptions import TransactionNotFound


# TODO: Revisit 'AttributeDict' to avoid using this function
def to_dict(dict_to_parse):
    # convert any 'AttributeDict' type found to 'dict'
    parsed_dict = dict(dict_to_parse)
    for key, val in parsed_dict.items():
        # check for nested dict structures to iterate through
        if 'dict' in str(type(val)).lower():
            parsed_dict[key] = to_dict(val)
        # convert 'HexBytes' type to 'str'
        elif 'HexBytes' in str(type(val)):
            parsed_dict[key] = val.hex()
    return parsed_dict


def arb_blocks(count):
    # ignoring future warning of "append" being deprecated
    warnings.filterwarnings("ignore")
    blocks_df = pd.DataFrame()

    for block_num in range(count):
        block = w3.eth.get_block(block_num)
        block_dict = to_dict(block)
        if (block_num % 1000) == 0:
            print((block_num/100000)*100, "Percent - (Blocks)")
        if blocks_df is None:
            blocks_df = pd.DataFrame.from_dict(block_dict, orient="columns")
        blocks_df = blocks_df.append(block_dict, ignore_index=True)

    warnings.filterwarnings("default")

    # Rename headers before uploading to clickhouse
    blocks_df = blocks_df.rename(columns={'gasLimit': 'gas_limit', 'parentHash': 'parent_hash', 'gasUsed': 'gas_used',
                                          'extraData': 'extra_data', 'logsBloom': 'logs_bloom', 'mixHash': 'mix_hash',
                                          'sha3Uncles': 'sha3_uncles', 'totalDifficulty': 'total_difficulty',
                                          'receiptsRoot': 'receipts_root', 'stateRoot': 'state_root',
                                          'l1BlockNumber': 'l1_block_number', 'transactionsRoot': 'transactions_root'})
    blocks_df = blocks_df.astype(str)
    return blocks_df


def arb_transactions(count):
    # ignoring future warning of "append" being deprecated
    warnings.filterwarnings("ignore")
    txns_df = pd.DataFrame()

    for txns_num in range(count):
        try:
            # Tried get_transaction_by_block([number], 3) but returned "not found"
            txn = w3.eth.get_transaction_by_block(txns_num, 0)
        except TransactionNotFound:
            continue

        block_dict = to_dict(txn)
        if txns_df is None:
            txns_df = pd.DataFrame.from_dict(block_dict, orient="columns")
        if (txns_num % 1000) == 0:
            print((txns_num / 100000) * 100, "Percent - (Transactions)")
        txns_df = txns_df.append(block_dict, ignore_index=True)

    warnings.filterwarnings("default")

    txns_df = txns_df.rename(columns={'blockHash': 'block_hash', 'blockNumber': 'block_number',
                                      'gasPrice': 'gas_price', 'transactionIndex': 'transaction_index'})

    # TODO: Review schema and dataframe datatypes to iron out datatype issue
    # "value" was returning floats causing database upload issues, therefore performed quick casting workaround
    txns_df = txns_df.astype(str)
    txns_df = txns_df.astype({"value": float})
    txns_df = txns_df.astype({"value": int})
    txns_df = txns_df.astype({"value": object})
    return txns_df


def create_tables(database):
    # blocks_raw table matching "ethereum-etl"
    sql = f'''
            CREATE TABLE IF NOT EXISTS {database}.blocks_raw
            (
                difficulty Int32,
                extra_data String,
                gas_limit Int64,
                gas_used Int32,
                hash String,
                l1_block_number String,
                logs_bloom String,
                miner String,
                mix_hash String,
                nonce String,
                number Int32,
                parent_hash String,
                receipts_root String,
                sha3_uncles String,
                size Int64,
                state_root String,
                timestamp String,
                transactions String,
                transactions_root String,
                uncles String
            )
            ENGINE = MergeTree
            PRIMARY KEY (number, hash)
            ORDER BY (number, hash)
            '''
    chClient.execute(sql)

    # transactions_raw matching "ethereum-etl"
    sql = f'''
        CREATE TABLE IF NOT EXISTS {database}.transactions_raw
        (
            block_hash String,
            block_number Int32,
            from String,
            gas Int32,
            gas_price Int32,
            hash String,
            input String,
            nonce String,
            to String,
            transaction_index Int32,
            value Int32,
            type String,
            v Int32,
            r String,
            s String
        )
        ENGINE = MergeTree
        PRIMARY KEY (block_number, hash)
        ORDER BY (block_number, hash)
        '''
    chClient.execute(sql)
    return None


def insert_into_blocks_database(dataframe, database):
    chClient.insert_dataframe(
        f'INSERT INTO {database}.blocks_raw (difficulty, extra_data, gas_limit, gas_used, hash, '
        'l1_block_number, logs_bloom, miner, mix_hash, nonce, number, parent_hash, receipts_root, sha3_uncles, size, '
        'state_root, timestamp, transactions, transactions_root, uncles) VALUES',
        dataframe)
    return None


# TODO: Merge insert_into_[databases]


def insert_into_transactions_database(dataframe, database):
    chClient.insert_dataframe(
        f'INSERT INTO {database}.transactions_raw (block_hash, block_number, from, gas, gas_price, hash, input, '
        'nonce, to, transaction_index, value, type, v, r , s) VALUES',
        dataframe)
    return None


if __name__ == '__main__':
    # TODO: Set up .ini file to store credentials on local drive

    # Insert node address
    w3 = Web3(HTTPProvider('[your_node_address]'))

    # Insert database address/credentials
    db_name = '[your_database]'
    pw = '[your_password]'
    chClient = Client('[your_cloud]', user='admin', password=pw, port=9440, secure='y',
                      verify=False,
                      database=db_name, settings={'use_numpy': True})

    # Create blocks and transactions tables on clickhouse
    create_tables(db_name)

    # Get first one hundred thousand blocks and transactions data for Arbitrum
    first_100k = 100000

    # Blocks
    blocks = arb_blocks(first_100k)

    insert_into_blocks_database(blocks, db_name)

    # Deleting in case there are memory constraints on device
    del blocks

    # Transactions
    transactions = arb_transactions(first_100k)

    insert_into_transactions_database(transactions, db_name)

    del transactions

    print("Complete")
