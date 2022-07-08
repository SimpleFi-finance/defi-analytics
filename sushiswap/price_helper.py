from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
USDC_ETH_PAIR = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WETH_DECIMALS = 18
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDC_DECIMALS = 6

class PriceProvider:
    def __init__(self):
        transport = AIOHTTPTransport(url=SUSHISWAP_ENDPOINT)
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=120)
        self._decimals = {}
        self._decimals[WETH] = WETH_DECIMALS
        self._decimals[USDC] = USDC_DECIMALS
        self._weth_pairs = {}
        self._weth_pairs[USDC] = USDC_ETH_PAIR

    def getEthPriceinUSD(self, block):
        """Calculate ETH price in USD at a given block.
        Sushiswap subgraph is used to fetch WETH-USDC pool reserves.
        """

        query = self._load_query('queries/pair_reserves.graphql')
        vars = {"block": block, "market": USDC_ETH_PAIR}
        response = self._client.execute(query, variable_values=vars)

        if response['market'] is None:
            return 0

        reserves = response['market']['inputTokenTotalBalances']
        tokenA = reserves[0].split("|")[0]
        tokenA_reserve = int(reserves[0].split("|")[2])
        tokenB_reserve = int(reserves[1].split("|")[2])

        if(tokenA == WETH):
            return (tokenB_reserve * pow(10, (-1) * USDC_DECIMALS)) / (tokenA_reserve * pow(10, (-1) * WETH_DECIMALS))
        else:
            return (tokenA_reserve * pow(10, (-1) * USDC_DECIMALS)) / (tokenB_reserve * pow(10, (-1) * WETH_DECIMALS))

    def getTokenPriceinUSD(self, token, block):
        """Calculate custom token price in USD at a given block.
        Sushiswap subgraph is used to fetch token reserves.
        """

        eth_price = self.getEthPriceinUSD(block)
        if token == WETH:
            return eth_price

        weth_pair = self.getWethPairForToken(token)

        if weth_pair is None:
            #TODO use some other pair
            return None

        query = self._load_query('queries/pair_reserves.graphql')
        vars = {"block": block, "market": weth_pair}
        response = self._client.execute(query, variable_values=vars)

        if response['market'] is None:
            return 0

        reserves = response['market']['inputTokenTotalBalances']
        tokenA = reserves[0].split("|")[0]
        tokenA_reserve = int(reserves[0].split("|")[2])

        tokenB = reserves[1].split("|")[0]
        tokenB_reserve = int(reserves[1].split("|")[2])

        if(tokenA == WETH):
            token_price_in_eth = (tokenA_reserve * pow(10, (-1) * self.decimals(WETH))) /(tokenB_reserve * pow(10, (-1) * self.decimals(tokenB)))
            token_price_in_usd = token_price_in_eth * eth_price
            return token_price_in_usd
        else:
            token_price_in_eth = (tokenB_reserve * pow(10, (-1) * self.decimals(WETH))) / (tokenA_reserve * pow(10, (-1) * self.decimals(tokenA)))
            token_price_in_usd = token_price_in_eth * eth_price
            return token_price_in_usd
    
    def getMarketSnapshotsForBlocks(self, market, blocks):
        blocks = sorted(set(blocks))
        snapshots = {}
        query = self._load_query('queries/pair_reserves_blocks.graphql')
        firstSnapshot = None
        n = 100

        for i in range(0, len(blocks), n):
            vars = {"market": market, "blocks": blocks[i:i+n]}
            response = self._client.execute(query, variable_values=vars)
            if response["marketSnapshots"] and firstSnapshot == None:
                firstSnapshot = response["marketSnapshots"][0]
            for snapshot in response["marketSnapshots"]:
                snapshots[int(snapshot["blockNumber"])] = snapshot
        
        lastSnapshot = firstSnapshot
        for block in blocks:
            if not block in snapshots:
                snapshots[block] = lastSnapshot
            else:
                lastSnapshot = snapshots[block]

        return snapshots

    def getEthPriceinUSDForBlocks(self, blocks):
        """Calculate ETH price in USD for all blocks between given start and end block.
        Sushiswap subgraph is used to fetch WETH-USDC pool reserves.
        """
        print("Collect ETH prices...")

        blocks = sorted(set(blocks))
        marketSnapshots = self.getMarketSnapshotsForBlocks(USDC_ETH_PAIR, blocks)

        if not marketSnapshots:
            return {}

        eth_prices = {}
        for block in blocks:
            reserves = marketSnapshots[block]['inputTokenBalances']
            tokenA = reserves[0].split("|")[0]
            tokenA_reserve = int(reserves[0].split("|")[2])
            tokenB_reserve = int(reserves[1].split("|")[2])

            if(tokenA == WETH):
                eth_prices[block] = (tokenB_reserve * pow(10, (-1) * USDC_DECIMALS)) / (tokenA_reserve * pow(10, (-1) * WETH_DECIMALS))
            else:
                eth_prices[block] = (tokenA_reserve * pow(10, (-1) * USDC_DECIMALS)) / (tokenB_reserve * pow(10, (-1) * WETH_DECIMALS))
          
        return eth_prices

    def getTokenPriceinUSDForBlocks(self, token, blocks, eth_prices):
        """Calculate custom token price in USD for all blocks between given start and end block.
        Sushiswap subgraph is used to fetch token reserves.
        """
        blocks = sorted(set(blocks))

        ## collect ETH prices if empty dict is provided
        if len(eth_prices) == 0:
            eth_prices = self.getEthPriceinUSDForBlocks(blocks)
        if token == WETH:
            return eth_prices

        weth_pair = self.getWethPairForToken(token)

        if weth_pair is None:
            #TODO use some other pair
            print("WETH pair not found for token", token)
            return None

        print("Collect", token, "prices...")

        marketSnapshots = self.getMarketSnapshotsForBlocks(weth_pair, blocks)

        if not marketSnapshots:
            return {}

        token_prices = {}
        for block in blocks:
            reserves = marketSnapshots[block]['inputTokenBalances']
            tokenA = reserves[0].split("|")[0]
            tokenA_reserve = int(reserves[0].split("|")[2])

            tokenB = reserves[1].split("|")[0]
            tokenB_reserve = int(reserves[1].split("|")[2])

            if tokenA_reserve == 0 or tokenB_reserve == 0:
                token_prices[block] = 0
                continue

            if(tokenA == WETH):
                token_price_in_eth = (tokenA_reserve * pow(10, (-1) * self.decimals(WETH))) /(tokenB_reserve * pow(10, (-1) * self.decimals(tokenB)))
            else:
                token_price_in_eth = (tokenB_reserve * pow(10, (-1) * self.decimals(WETH))) / (tokenA_reserve * pow(10, (-1) * self.decimals(tokenA)))

            token_prices[block] = token_price_in_eth * eth_prices[block]
        
        return token_prices

    def getWethPairForToken(self, token):
        """Find a WETH pair for token, if exists.
        Returns None if there is no such pair.
        """

        weth_pair = self._weth_pairs.get(token)
        if weth_pair != None:
            return weth_pair

        query = self._load_query('queries/get_eth_pair_for_token.graphql')
        vars = {"token": token}
        response = self._client.execute(query, variable_values=vars)

        if response['markets'] == None or len(response['markets']) == 0:
            return None

        weth_pair = response['markets'][0]['id']
        self._weth_pairs[token] = weth_pair

        return weth_pair


    def decimals(self, token_address):
        """Get number of decimals for token.
        Query subgraph if info is not stored locally.
        """

        num_of_decimals = self._decimals.get(token_address)
        if num_of_decimals != None:
            return num_of_decimals

        query = self._load_query('queries/get_token_decimals.graphql')
        vars = {"id": token_address}
        response = self._client.execute(query, variable_values=vars)

        if response['token'] == None:
            self._decimals[token_address] = None
            return None

        num_of_decimals = response['token']['decimals']
        self._decimals[token_address] = num_of_decimals

        return num_of_decimals


    def _load_query(self, path):
        with open(path) as f:
            return gql(f.read())



