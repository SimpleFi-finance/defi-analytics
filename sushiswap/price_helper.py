from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
USDC_ETH_PAIR = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WETH_DECIMALS = 18
USDC_DECIMALS = 6

class PriceProvider:
    def __init__(self):
        transport = AIOHTTPTransport(url=SUSHISWAP_ENDPOINT)
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=30)

    def getEthPriceinUSD(self, block):
        """Calculate ETH price in USD at a given block.
        Sushiswap subgraph is used to fetch WETH-USDC pool reserves.
        """

        query = self._load_query('pair_reserves.graphql')
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

        query = self._load_query('pair_reserves.graphql')
        vars = {"block": block, "market": weth_pair['id']}
        response = self._client.execute(query, variable_values=vars)

        if response['market'] is None:
            return 0

        reserves = response['market']['inputTokenTotalBalances']
        tokenA = reserves[0].split("|")[0]
        tokenA_reserve = int(reserves[0].split("|")[2])
        tokenA_decimals = int(weth_pair['inputTokens'][0]['decimals'])
        tokenB_reserve = int(reserves[1].split("|")[2])
        tokenB_decimals = int(weth_pair['inputTokens'][1]['decimals'])

        if(tokenA == WETH):
            token_price_in_eth = (tokenA_reserve * pow(10, (-1) * WETH_DECIMALS)) /(tokenB_reserve * pow(10, (-1) * tokenB_decimals))
            token_price_in_usd = token_price_in_eth * eth_price
            return token_price_in_usd
        else:
            token_price_in_eth = (tokenB_reserve * pow(10, (-1) * WETH_DECIMALS)) / (tokenA_reserve * pow(10, (-1) * tokenA_decimals))
            token_price_in_usd = token_price_in_eth * eth_price
            return token_price_in_usd


    def getWethPairForToken(self, token):
        query = self._load_query('get_eth_pair_for_token.graphql')
        vars = {"token": token}
        response = self._client.execute(query, variable_values=vars)

        if response['markets'] == None:
            return None

        return response['markets'][0]


    def _load_query(self, path):
        with open(path) as f:
            return gql(f.read())



