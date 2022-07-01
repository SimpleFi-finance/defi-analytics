from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from price_helper import PriceProvider


price_provider = PriceProvider()

class GraphClient:

    def __init__(self, endpoint):
        self._subgraph_endpoint = endpoint
        transport = AIOHTTPTransport(url=endpoint)
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=120)
    
    def _load_query(self, filename):
        with open(filename) as f:
            return gql(f.read())

    def runQuery(self, filename, args):
        query = self._load_query(filename)
        response = self._client.execute(query, variable_values=args)
        return response

class SushiswapFarmsClient(GraphClient):
    
    def __init__(self):
      super().__init__("https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap-farms")

    def getMarketForLPToken(self, lpToken):
      response = self.runQuery("queries/get_farm_for_lp_token.graphql", {"lpToken": lpToken})
      return response["markets"][0]["id"]

    def getTransactionsOfClosedPositions(self, marketId):
        """Fetch closed positions from the Sushiswap subgraph.
        Return a dictionary where key is account address of position and value is list of TXs.
        """
        
        raw_positions = {}
        lastID = ""

        while True:
            vars = {"lastID": lastID, "market": marketId}
            query = self._load_query('queries/get_raw_closed_positions.graphql')
            response = self._client.execute(query, variable_values=vars)
            if not response['positions']:
                break

            for position in response['positions']:
                if not position['accountAddress'] in raw_positions: raw_positions[position['accountAddress']] = []
                for positionSnapshot in position['history']:
                    tx = positionSnapshot['transaction']
                    tx["blockNumber"] = int(tx["blockNumber"])
                    raw_positions[position['accountAddress']].append(tx)
            
            lastID = response['positions'][-1]['id']
            print("Processed farm positions:", len(raw_positions))
        
        return raw_positions
    
    def getFarmTransactionsForPositions(self, farm_address, farm_transactions, pool_transactions):
        farm_transactions_for_positions = {}

        transaction_ids = []  # Required to avoid duplicate transactions 
        for position_id in pool_transactions.keys():
            account_address = pool_transactions[position_id][0]["accountAddress"]
            if not account_address in farm_transactions: continue
            farm_transactions_for_account = farm_transactions[account_address]

            for tx in pool_transactions[position_id]:
                if (tx["transactionType"] == "TRANSFER_OUT" and tx["transferredTo"] == farm_address) or (tx["transactionType"] == "TRANSFER_IN" and tx["transferredFrom"] == farm_address):
                    transactions = list(filter(lambda x: x["transactionHash"] == tx["transactionHash"], farm_transactions_for_account))
                    if transactions:
                        if not position_id in farm_transactions_for_positions: farm_transactions_for_positions[position_id] = []
                        for transaction in transactions:
                            if not transaction["id"] in transaction_ids:
                                farm_transactions_for_positions[position_id].append(transaction)
                                transaction_ids.append(transaction["id"])

        return farm_transactions_for_positions
    
    def addRewardValueInUSD(self, farm_transactions):
        blocks = []
        rewardTokens = []
        transactions_with_prices = {}
        for position_id in farm_transactions.keys():
            transactions = []
            for tx in farm_transactions[position_id]:
                rewards = list(map(lambda x: self.parseTokenBalance(x), tx["rewardTokenAmounts"]))
                nonZero = list(filter(lambda x: x["amount"] > 0, rewards))
                if nonZero:
                    tx["rewardTokenAmounts"] = rewards
                    transactions.append(tx)
                    blocks.append(tx["blockNumber"])
                    for reward in nonZero:
                        if not reward["token"] in rewardTokens: rewardTokens.append(reward["token"])
            transactions_with_prices[position_id] = transactions
        
        # Fetch prices only once for all the blocks we need
        prices = {}
        for token in rewardTokens:
            prices[token] = price_provider.getTokenPriceinUSDForBlocks(token, blocks)
        
        for position_id in transactions_with_prices.keys():
            transactions = []
            for tx in transactions_with_prices[position_id]:
                rewards = []
                for reward in tx["rewardTokenAmounts"]:
                    token = reward["token"]
                    block = tx["blockNumber"]
                    reward["price"] = prices[token][block]
                    reward["valueInUSD"] = reward["amount"] * reward["price"]
                    rewards.append(reward)
                tx["rewardTokenAmounts"] = rewards
                transactions.append(tx)
            transactions_with_prices[position_id] = transactions
        return transactions_with_prices

    def parseTokenBalance(self, tokenBalance):
        parts = tokenBalance.split("|")
        token = parts[0]
        amount = int(parts[2]) * pow(10, (-1) * price_provider.decimals(token))
        return {
          "token": token,
          "amount": amount
        }