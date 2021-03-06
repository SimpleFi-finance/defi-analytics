import csv
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from price_helper import PriceProvider
from datetime import datetime
import time

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
MASTERCHEFS = ["0xc2edad668740f1aa35e4d8f227fb8e17dca888cd", "0xef0881ec094552b2e128cf945ef17a6752b4ec5d"]

class PositionHandler:

    def __init__(self, endpoint):
        self._subgraph_endpoint = endpoint
        transport = AIOHTTPTransport(url=endpoint)
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=120)

    def getAllEthMarkets(self):
        """Fetch all markets (Sushiswap pairs) where one input token is WETH.
        """

        markets = {}
        lastID = ""

        while True:
            vars = {"lastID": lastID}
            query = self._load_query('queries/get_all_eth_markets.graphql')
            response = self._client.execute(query, variable_values=vars)
            if not response['markets']:
                break

            for market in response['markets']:
                markets[market['id']] = True

            lastID = response['markets'][-1]['id']
            print("Processed markets:", len(response["markets"]))

        return markets

    def getRawClosedPositions(self, market):
        """Fetch closed positions from the Sushiswap subgraph.
        Return a dictionary where key is position ID and value list of TXs.
        """
        
        raw_positions = {}
        lastID = ""

        while True:
            vars = {"lastID": lastID, "market": market}
            query = self._load_query('queries/get_raw_closed_positions.graphql')
            response = self._client.execute(query, variable_values=vars)
            if not response['positions']:
                break

            for position in response['positions']:
                raw_positions[position['id']] = []
                for positionSnapshot in position['history']:
                    tx = positionSnapshot['transaction']
                    tx["accountAddress"] = position["accountAddress"]
                    tx["blockNumber"] = int(tx["blockNumber"])
                    raw_positions[position['id']].append(tx)

            lastID = response['positions'][-1]['id']
            print("Processed positions:", len(raw_positions))
        
        ordered_raw_positions = dict(sorted(raw_positions.items(), key=lambda item: (item[0].rsplit('-', 1)[0], int(item[0].split("-")[-1]))))
        return ordered_raw_positions

    def getAllRawClosedPositions(self):
        """Fetch all closed positions from the Sushiswap subgraph, in ETH markets.
        Return a dictionary where key is position ID and value list of TXs.
        """
        eth_markets = self.getAllEthMarkets()
        print("Found {0} ETH markets".format(len(eth_markets)))

        raw_positions = {}
        lastID = ""

        while True:
            vars = {"lastID": lastID}
            query = self._load_query('queries/get_all_raw_closed_positions.graphql')
            response = self._client.execute(query, variable_values=vars)
            if not response['positions']:
                break

            for position in response['positions']:
                raw_positions[position['id']] = []
                for positionSnapshot in position['history']:
                    tx = positionSnapshot['transaction']
                    tx["accountAddress"] = position["accountAddress"]
                    tx["blockNumber"] = int(tx["blockNumber"])
                    raw_positions[position['id']].append(tx)

            lastID = response['positions'][-1]['id']
            print("Processed positions:", len(raw_positions))

        raw_positions_in_weth_markets = {k:v for (k,v) in raw_positions.items() if k.split('-')[1] in eth_markets.keys()}
        print("All positions: ", len(raw_positions))
        print("Positions in WETH markets: ", len(raw_positions_in_weth_markets))

        ordered_raw_positions_in_weth_markets = dict(sorted(raw_positions_in_weth_markets.items(), key=lambda item: (item[0].rsplit('-', 1)[0], int(item[0].split("-")[-1]))))
        return ordered_raw_positions_in_weth_markets

    def mergePositionsByHistory(self, raw_positions):
        """Merge positions which are part of same user history.
        When user transfers his LP tokens to i.e. MasterChef, position is marked as closed. However,
        we want to connect it with the next position where user claims back his LP tokens and then redeems them.
        """

        merged_positions = {}
        processed_positions = {}

        ordered_raw_positions = dict(sorted(raw_positions.items(), key=lambda item: (item[0].rsplit('-', 1)[0], int(item[0].split("-")[-1]))))

        for position_id in ordered_raw_positions.keys():
            # don't process position which was already processed as part of position merging
            if processed_positions.get(position_id) == True:
                continue

            txs = raw_positions[position_id]
            first_tx = txs[0]
            last_tx = txs[-1]

            # first TX has to be of type INVEST
            if first_tx['transactionType'] != "INVEST":
                continue

            # if last TX is REDEEM then position is completed
            if last_tx['transactionType'] == "REDEEM":
                # skip positions where LP tokens were transferred out somewhere else then masterchef 
                if any((tx['transactionType'] == "TRANSFER_OUT" and tx["transferredTo"] not in MASTERCHEFS) for tx in txs):
                    continue

               # skip positions where LP tokens were transferred in from somewhere else then masterchef 
                if any((tx['transactionType'] == "TRANSFER_IN" and tx["transferredFrom"] not in MASTERCHEFS) for tx in txs):
                    continue

                merged_positions[position_id] = raw_positions[position_id]
                processed_positions[position_id] = True
                continue

            # if last TX is TRANSFER_OUT (to masterchef) then search for subsequent TXs (by ID) to make position complete
            if last_tx['transactionType'] == "TRANSFER_OUT" and last_tx["transferredTo"] in MASTERCHEFS:
                expanded_position = raw_positions[position_id]
                expanded_position_complete = False

                # ie. 5 for position '0x0000000000000d9054f605ca65a2647c2b521422-0xb84c45174bfc6b8f3eaecbae11dee63114f5c1b2-INVESTMENT-5'
                current_position_counter = int(position_id.split("-")[-1])
                while True:
                    # increase position ID by 1
                    next_position_id = position_id.rsplit('-', 1)[0] + "-" + str(current_position_counter + 1)
                    next_position = raw_positions.get(next_position_id)

                    # if there's no next position don't add anything
                    if next_position == None:
                        break

                    # if last TX is not REDEEM then loop again to pick up and merge next position
                    elif next_position[-1]['transactionType'] != "REDEEM":
                        current_position_counter = int(next_position_id.split("-")[-1])
                        expanded_position = expanded_position + next_position
                        processed_positions[next_position_id] = True
                        continue

                    # position has been completed if last TX is REDEEM
                    else:
                        expanded_position = expanded_position + next_position
                        expanded_position_complete = True
                        processed_positions[next_position_id] = True
                        break

                # add completed position to list
                if expanded_position_complete:
                    merged_positions[position_id] = expanded_position
                    processed_positions[position_id] = True
        
        return merged_positions

    def filterOutPositionsWithMultipleInvestsOrRedeems(self, positions):
        """Return positions which have 1 Invest and 1 Redeem TX """

        filtered_positions = {}
        for position_id in positions.keys():
            txs = positions[position_id]
            invest_count = sum(map(lambda x : x['transactionType'] == 'INVEST', txs))
            redeem_count = sum(map(lambda x : x['transactionType'] == 'REDEEM', txs))

            if invest_count == 1 and redeem_count == 1:
                filtered_positions[position_id] = positions[position_id]
        
        return filtered_positions

    
    def calculateProfitabilityOfPoolPositions(self, positions, farm_transactions):
        """Return dict of position profitability stats.
        For every position calculate net gains, ROI gains and pool vs HODL. difference.
        """
        start_time = time.time()

        price_provider = PriceProvider()
        position_stats = {}

        ## collect all blocks
        blocks = set()
        for pos_id in positions.keys():
            txs = positions[pos_id]
            blocks.update([tx['blockNumber'] for tx in txs if tx['transactionType'] == 'INVEST' or tx['transactionType'] == 'REDEEM'])

        ## collect ETH prices
        prices = {}
        prices[WETH] = price_provider.getEthPriceinUSDForBlocks(blocks)

        ## extract input tokens
        tokenA = positions[list(positions.keys())[0]][0]['inputTokenAmounts'][0].split("|")[0]
        tokenB = positions[list(positions.keys())[0]][0]['inputTokenAmounts'][1].split("|")[0]

        ## collect prices for tokenA, tokenB
        prices[tokenA] = price_provider.getTokenPriceinUSDForBlocks(tokenA, blocks, prices[WETH])
        prices[tokenB] = price_provider.getTokenPriceinUSDForBlocks(tokenB, blocks, prices[WETH])

        ## do calcs for every position
        for pos_id in positions.keys():
            txs = positions[pos_id]

            ## get position start/end info
            position_start_block, position_end_block, position_start_date, position_end_date = self._getPositionTimestamps(txs)

            # it's mostly contracts opening/closing positions in same day so skip it
            if position_start_date == position_end_date:
                continue

            ## sum all investments
            position_investment_value, tokenA_total_amount_invested, tokenB_total_amount_invested = self._sumInvestments(
                txs, prices, tokenA, tokenB, price_provider)

            ## sum all redemptions
            position_redemption_value, tokenA_price, tokenB_price = self._sumRedemptions(
                txs, prices, tokenA, tokenB, price_provider)

            ## calculate gains
            position_redemption_value_if_held = tokenA_total_amount_invested * tokenA_price + tokenB_total_amount_invested * tokenB_price

            pool_net_gain = position_redemption_value - position_investment_value
            hodl_net_gain = position_redemption_value_if_held - position_investment_value

            pool_roi = pool_net_gain / position_investment_value
            hodl_roi = hodl_net_gain / position_investment_value
            pool_vs_hodl_roi = (position_redemption_value - position_redemption_value_if_held) / position_redemption_value_if_held

            # Calculation total farm rewards
            claimed_rewards_in_USD = 0
            if pos_id in farm_transactions:
                for farm_tx in farm_transactions[pos_id]:
                    for reward in farm_tx["rewardTokenAmounts"]:
                        claimed_rewards_in_USD = claimed_rewards_in_USD + reward["valueInUSD"]

            ## write stats
            position_stats[pos_id] = {
                'market': pos_id.split("-")[1],
                'account': pos_id.split("-")[0],
                'position_start_block': position_start_block,
                'position_end_block': position_end_block,
                'position_start_date': position_start_date,
                'position_end_date': position_end_date,
                'tokenA': tokenA,
                'tokenB': tokenB,
                'position_investment_value': position_investment_value,
                'position_redemption_value': position_redemption_value,
                'position_redemption_value_if_held': position_redemption_value_if_held,
                'pool_net_gain': pool_net_gain,
                'hodl_net_gain': hodl_net_gain,
                'pool_roi': pool_roi,
                'hodl_roi': hodl_roi,
                'pool_vs_hodl_roi': pool_vs_hodl_roi,
                'claimed_rewards_in_USD': claimed_rewards_in_USD,
                'position_trade_counter': len(txs)
            }

            if(len(position_stats.keys()) % 10 == 0):
                print(len(position_stats.keys()))

        end_time = time.time()

        print("--- %s seconds ---" % (end_time - start_time))

        return position_stats


    def calculateProfitabilityOfAllPositions(self, positions, farm_transactions):
        """Return dict of position profitability stats.
        For every position calculate net gains, ROI gains and pool vs HODL. difference.
        """
        start_time = time.time()

        price_provider = PriceProvider()
        position_stats = {}

        ## collect all blocks
        all_blocks = set()
        for pos_id in positions.keys():
            txs = positions[pos_id]
            all_blocks.update([tx['blockNumber'] for tx in txs if tx['transactionType'] == 'INVEST' or tx['transactionType'] == 'REDEEM'])

        ## collect ETH prices
        print("Collect ETH prices")
        prices = {}
        prices[WETH] = price_provider.getEthPriceinUSDForBlocks(all_blocks)

        ## do calcs for every position
        for pos_id in positions.keys():
            txs = positions[pos_id]

            ## extract input tokens
            tokenA = txs[0]['inputTokenAmounts'][0].split("|")[0]
            tokenB = txs[0]['inputTokenAmounts'][1].split("|")[0]

            ## get tokenA prices
            if prices.get(tokenA) == None:
                blocks = set()
                for pos_id in positions.keys():
                    txs = positions[pos_id]
                    blocks.update([tx['blockNumber'] for tx in txs if tx['transactionType'] == 'INVEST' or tx['transactionType'] == 'REDEEM'])
                prices[tokenA] = price_provider.getTokenPriceinUSDForBlocks(tokenA, blocks, prices[WETH])

            ## get tokenB prices
            if prices.get(tokenB) == None:
                blocks = set()
                for pos_id in positions.keys():
                    txs = positions[pos_id]
                    blocks.update([tx['blockNumber'] for tx in txs if tx['transactionType'] == 'INVEST' or tx['transactionType'] == 'REDEEM'])
                prices[tokenB] = price_provider.getTokenPriceinUSDForBlocks(tokenB, blocks, prices[WETH])

            ## get position start/end info
            position_start_block, position_end_block, position_start_date, position_end_date = self._getPositionTimestamps(txs)

            # don't handle one TX position opening/closing
            if position_start_block == position_end_block:
                continue

            ## sum all investments
            position_investment_value, tokenA_total_amount_invested, tokenB_total_amount_invested = self._sumInvestments(
                txs, prices, tokenA, tokenB, price_provider)

            ## sum all redemptions
            position_redemption_value, tokenA_price, tokenB_price = self._sumRedemptions(
                txs, prices, tokenA, tokenB, price_provider)

            ## calculate gains
            position_redemption_value_if_held = tokenA_total_amount_invested * tokenA_price + tokenB_total_amount_invested * tokenB_price

            pool_net_gain = position_redemption_value - position_investment_value
            hodl_net_gain = position_redemption_value_if_held - position_investment_value

            pool_roi = pool_net_gain / position_investment_value
            hodl_roi = hodl_net_gain / position_investment_value
            pool_vs_hodl_roi = pool_roi - hodl_roi

            # Calculation total farm rewards
            claimed_rewards_in_USD = 0
            if pos_id in farm_transactions:
                for farm_tx in farm_transactions[pos_id]:
                    for reward in farm_tx["rewardTokenAmounts"]:
                        claimed_rewards_in_USD = claimed_rewards_in_USD + reward["valueInUSD"]

            ## write stats
            position_stats[pos_id] = {
                'market': pos_id.split("-")[1],
                'account': pos_id.split("-")[0],
                'position_start_block': position_start_block,
                'position_end_block': position_end_block,
                'position_start_date': position_start_date,
                'position_end_date': position_end_date,
                'tokenA': tokenA,
                'tokenB': tokenB,
                'position_investment_value': position_investment_value,
                'position_redemption_value': position_redemption_value,
                'position_redemption_value_if_held': position_redemption_value_if_held,
                'pool_net_gain': pool_net_gain,
                'hodl_net_gain': hodl_net_gain,
                'pool_roi': pool_roi,
                'hodl_roi': hodl_roi,
                'pool_vs_hodl_roi': pool_vs_hodl_roi,
                'claimed_rewards_in_USD': claimed_rewards_in_USD,
                'position_trade_counter': len(txs)
            }

            if(len(position_stats.keys()) % 10 == 0):
                print(len(position_stats.keys()))

        end_time = time.time()

        print("--- %s seconds ---" % (end_time - start_time))

        return position_stats

    def writeProfitabilityStatsToCsv(self, stats, filename):
        """Write all the collected info to CSV file"""

        f = open(filename, "w")
        writer = csv.DictWriter(f, fieldnames=[
                'market',
                'account',
                'position_start_block',
                'position_end_block',
                'position_start_date',
                'position_end_date',
                'tokenA',
                'tokenB',
                'position_investment_value',
                'position_redemption_value',
                'position_redemption_value_if_held',
                'pool_net_gain',
                'hodl_net_gain',
                'pool_roi',
                'hodl_roi',
                'pool_vs_hodl_roi',
                'claimed_rewards_in_USD',
                'position_trade_counter'
        ])
        writer.writeheader()

        for position_id in stats.keys():
            writer.writerow(stats[position_id])
        f.close()

    def _load_query(self, path):
        with open(path) as f:
            return gql(f.read())

    def _sumInvestments(self, txs, prices, tokenA, tokenB, price_provider):
        position_investment_value = 0
        investTXs = [tx for tx in txs if tx['transactionType'] == 'INVEST']

        tokenA_total_amount_invested = 0
        tokenB_total_amount_invested = 0
        for tx in investTXs:
            block = int(tx['blockNumber'])

            tokenA_amount = int(tx['inputTokenAmounts'][0].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenA))
            tokenA_price = prices[tokenA][block]
            if(tokenA_price == None):
                continue
            position_investment_value += tokenA_amount * tokenA_price
            tokenA_total_amount_invested += tokenA_amount

            tokenB_amount = int(tx['inputTokenAmounts'][1].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenB))
            tokenB_price = prices[tokenB][block]
            if(tokenB_price == None):
                continue
            position_investment_value += tokenB_amount * tokenB_price
            tokenB_total_amount_invested += tokenB_amount

        return position_investment_value, tokenA_total_amount_invested, tokenB_total_amount_invested

    def _sumRedemptions(self, txs, prices, tokenA, tokenB, price_provider):
        redeemTXs = [tx for tx in txs if tx['transactionType'] == 'REDEEM']
        position_redemption_value = 0

        tokenA_price = 0
        tokenB_price = 0
        for tx in redeemTXs:
            block = int(tx['blockNumber'])

            tokenA_amount = int(tx['inputTokenAmounts'][0].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenA))
            tokenA_price = prices[tokenA][block]
            if(tokenA_price == None):
                continue
            position_redemption_value += tokenA_amount * tokenA_price

            tokenB_amount = int(tx['inputTokenAmounts'][1].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenB))
            tokenB_price = prices[tokenB][block]
            if(tokenB_price == None):
                continue
            position_redemption_value += tokenB_amount * tokenB_price

        return position_redemption_value, tokenA_price, tokenB_price

    def _getPositionTimestamps(self, txs):
        position_start_block = txs[0]['blockNumber']
        position_end_block = txs[-1]['blockNumber']
        start_timestamp = int(txs[0]['timestamp'])
        position_start_date = datetime.strftime(datetime.fromtimestamp(start_timestamp), '%Y-%m-%d')
        end_timestamp = int(txs[-1]['timestamp'])
        position_end_date = datetime.strftime(datetime.fromtimestamp(end_timestamp), '%Y-%m-%d')

        return position_start_block,position_end_block,position_start_date,position_end_date