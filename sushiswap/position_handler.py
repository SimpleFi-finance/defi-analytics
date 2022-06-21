import csv
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from price_helper import PriceProvider

class PositionHandler:

    def __init__(self, endpoint):
        self._subgraph_endpoint = endpoint
        transport = AIOHTTPTransport(url=endpoint)
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=120)


    def getRawClosedPositions(self, market):
        """Fetch closed positions from the Sushiswap subgraph.
        Return a dictionary where key is position ID and value list of TXs.
        """
        
        raw_positions = {}
        lastID = ""

        while True:
            vars = {"lastID": lastID, "market": market}
            query = self._load_query('get_raw_closed_positions.graphql')
            response = self._client.execute(query, variable_values=vars)
            if not response['positions']:
                break

            for position in response['positions']:
                raw_positions[position['id']] = []
                for positionSnapshot in position['history']:
                    tx = positionSnapshot['transaction']
                    raw_positions[position['id']].append(tx)
            
            lastID = response['positions'][-1]['id']
            print("Processed positions:", len(raw_positions))
        
        return raw_positions


    def mergePositionsByHistory(self, raw_positions):
        """Merge positions which are part of same user history.
        When user transfers his LP tokens to i.e. MasterChef, position is marked as closed. However,
        we want to connect it with the next position where user claims back his LP tokens and then redeems them.
        """

        merged_positions = {}
        processed_positions = {}

        for position_id in raw_positions.keys():
            # don't process position which was already processed as part of position merging
            if processed_positions.get(position_id) == True:
                continue

            first_tx = raw_positions[position_id][0]
            last_tx = raw_positions[position_id][-1]

            # first TX has to be of type INVEST
            if first_tx['transactionType'] != "INVEST":
                continue

            # if last TX is REDEEM then position is completed
            if last_tx['transactionType'] == "REDEEM":
                merged_positions[position_id] = raw_positions[position_id]
                processed_positions[position_id] = True
                continue

            # if last TX is TRANSFER_OUT then search for subsequent TXs (by ID) to make position complete
            if last_tx['transactionType'] == "TRANSFER_OUT":
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

    
    def calculateProfitabilityOfPositions(self, positions):
        """Return dict of position profitability stats.
        For every position calculate net gains, ROI gains and pool vs HODL. difference.
        """

        price_provider = PriceProvider()
        position_stats = {}

        for pos_id in positions.keys():
            txs = positions[pos_id]

            first_tx = txs[0]
            last_tx = txs[-1]

            position_start_block = int(first_tx['blockNumber'])
            position_end_block = int(last_tx['blockNumber'])

            inputTokenAmountsAStart = first_tx['inputTokenAmounts'][0].split("|")
            tokenA = inputTokenAmountsAStart[0]
            tokenA_amount_start = int(inputTokenAmountsAStart[2]) * pow(10, (-1) * price_provider.decimals(tokenA))
            tokenA_price_start = price_provider.getTokenPriceinUSD(tokenA, position_start_block)
            if(tokenA_price_start == None):
                continue

            tokenA_amount_end = int(last_tx['inputTokenAmounts'][0].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenA))
            tokenA_price_end = price_provider.getTokenPriceinUSD(tokenA, position_end_block)
            if(tokenA_price_end == None):
                continue

            inputTokenAmountsBStart = first_tx['inputTokenAmounts'][1].split("|")
            tokenB = inputTokenAmountsBStart[0]
            tokenB_amount_start = int(inputTokenAmountsBStart[2]) * pow(10, (-1) * price_provider.decimals(tokenB))
            tokenB_price_start = price_provider.getTokenPriceinUSD(tokenB, position_start_block)
            if(tokenB_price_start == None):
                continue

            tokenB_amount_end = int(last_tx['inputTokenAmounts'][1].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenB))
            tokenB_price_end = price_provider.getTokenPriceinUSD(tokenB, position_end_block)
            if(tokenB_price_end == None):
                continue

            position_start_value = tokenA_amount_start * tokenA_price_start + tokenB_amount_start * tokenB_price_start
            position_end_value = tokenA_amount_end * tokenA_price_end + tokenB_amount_end * tokenB_price_end
            position_end_value_if_held = tokenA_amount_start * tokenA_price_end + tokenB_amount_start * tokenB_price_end

            pool_net_gain = position_end_value - position_start_value
            hodl_net_gain = position_end_value_if_held - position_start_value

            pool_roi = pool_net_gain / position_start_value
            hodl_roi = hodl_net_gain / position_start_value
            pool_vs_hodl_roi = (position_end_value - position_end_value_if_held) / position_end_value

            position_stats[pos_id] = {
                'account': pos_id.split("-")[0],
                'position_start_block': position_start_block,
                'position_end_block': position_end_block,
                'tokenA': tokenA,
                'tokenA_amount_start': tokenA_amount_start,
                'tokenA_price_start': tokenA_price_start,
                'tokenA_amount_end': tokenA_amount_end,
                'tokenA_price_end': tokenA_price_end,
                'tokenB': tokenB,
                'tokenB_amount_start': tokenB_amount_start,
                'tokenB_price_start': tokenB_price_start,
                'tokenB_amount_end': tokenB_amount_end,
                'tokenB_price_end': tokenB_price_end,
                'position_start_value': position_start_value,
                'position_end_value': position_end_value,
                'position_end_value_if_held': position_end_value_if_held,
                'pool_net_gain': pool_net_gain,
                'hodl_net_gain': hodl_net_gain,
                'pool_roi': pool_roi,
                'hodl_roi': hodl_roi,
                'pool_vs_hodl_roi': pool_vs_hodl_roi
            }

            if(len(position_stats.keys()) % 10 == 0):
                print(len(position_stats.keys()))

        return position_stats

    
    def writeProfitabilityStatsToCsv(self, stats, filename):
        """Write all the collected info to CSV file"""

        f = open(filename, "w")
        writer = csv.DictWriter(f, fieldnames=[
                'account',
                'position_start_block',
                'position_end_block',
                'tokenA',
                'tokenA_amount_start',
                'tokenA_price_start',
                'tokenA_amount_end',
                'tokenA_price_end',
                'tokenB',
                'tokenB_amount_start',
                'tokenB_price_start',
                'tokenB_amount_end',
                'tokenB_price_end',
                'position_start_value',
                'position_end_value',
                'position_end_value_if_held',
                'pool_net_gain',
                'hodl_net_gain',
                'pool_roi',
                'hodl_roi',
                'pool_vs_hodl_roi'
        ])
        writer.writeheader()

        for position_id in stats.keys():
            writer.writerow(stats[position_id])
        f.close()


    def _load_query(self, path):
        with open(path) as f:
            return gql(f.read())