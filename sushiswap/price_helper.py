from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
USDC_ETH_PAIR = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

class PriceProvider:
    def __init__(self):
        transport = AIOHTTPTransport(url=SUSHISWAP_ENDPOINT)
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=30)

    def getEthPriceinUSD(self, block):
        """Calculate ETH price in USD at a given block.
        Sushiswap subgraph is used to fetch WETH-USDC pool reserves.
        """

        query = self._load_query('usdc_eth_reserves.graphql')
        vars = {"block": block, "market": USDC_ETH_PAIR}
        response = self._client.execute(query, variable_values=vars)

        if response['market'] is None:
            return 0

        reserves = response['market']['inputTokenTotalBalances']
        tokenA = reserves[0].split("|")[0]
        tokenA_reserve = int(reserves[0].split("|")[2])
        tokenB_reserve = int(reserves[1].split("|")[2])

        if(tokenA == WETH):
            return (tokenB_reserve * pow(10, -18)) / (tokenA_reserve * pow(10, -6))
        else:
            return (tokenA_reserve * pow(10, -6)) / (tokenB_reserve * pow(10, -18))


    def getTokenPriceinUSD(self, token, block):
        """Calculate custom token price in USD at a given block.
        Sushiswap subgraph is used to fetch WETH-USDC pool reserves.
        """

        if token == WETH:
            return self.getEthPriceinUSD(block)

        #TODO

    def getWethPairForToken(self, token):
        query = self._load_query('get_eth_pair_for_token.graphql')
        vars = {"token": token}
        response = self._client.execute(query, variable_values=vars)

        if response['markets'] == None:
            return None

        return response['markets'][0]['id']

    def _load_query(self, path):
        with open(path) as f:
            return gql(f.read())



