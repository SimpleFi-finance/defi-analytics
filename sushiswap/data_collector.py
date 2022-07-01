import csv
from position_handler import PositionHandler
from graph_clients import SushiswapFarmsClient

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
DAI_WETH_POOL = "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f"
LDO_WETH_POOL = "0xc558f600b34a5f69dd2f0d06cb8a88d829b7420a"
AAVE_WETH_POOL = "0xd75ea151a61d06868e31f8988d28dfe5e9df57b4"
USDC_WETH_POOL = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"
WBTC_WETH_POOL = "0xceff51756c56ceffca006cd410b03ffc46dd3a58"
USDT_WETH_POOL = "0x06da0fd433c1a5d7a4faa01111c044910a184553"
YFI_WETH_POOL = "0x088ee5007c98a9677165d78dd2109ae4a3d04d0c"

# Sushiswap Farms
SUSHISWAP_FARMS_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap-farms"
# DAI_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-2"
# LDO_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-109"
# AAVE_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-37"
# USDC_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-1"
# WBTC_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-21"
# USDT_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-0"
# YFI_WETH_POOL = "0xc2edad668740f1aa35e4d8f227fb8e17dca888cd-11"

def collect_data_for_pool(pool, filename):
    position_handler = PositionHandler(SUSHISWAP_ENDPOINT)

    raw_positions = position_handler.getRawClosedPositions(pool)
    print("Positions loaded from subgraph: {0}".format(len(raw_positions)))

    merged_positions = position_handler.mergePositionsByHistory(raw_positions)
    print("Positions after merging histories: {0}".format(len(merged_positions)))

    # Call farm data to get farm transactions list here
    # We can optimize it by fetching farm data and sending it to mergePositionByHistory to reduce number of loops
    farm_transactions = collect_data_for_farms(pool, merged_positions)
    print("Farm transactions for merged positions: {0}".format(len(farm_transactions)))

    print("Calculating profitability...")
    profitability_stats = position_handler.calculateProfitabilityOfPositions(merged_positions, farm_transactions)
    print("Profitability stats ready")

    position_handler.writeProfitabilityStatsToCsv(profitability_stats, filename)
    print("Stats written to {0}".format(filename))

def collect_data_for_farms(pool, positions):
    farm_client = SushiswapFarmsClient()
    market_id = farm_client.getMarketForLPToken(pool)
    print("Farm ID for pool: {0} is : {1}".format(market_id, pool))

    [farm_address, farm_id] = market_id.split("-")
    print("Farm Contract address: {0} and farm id : {1}".format(farm_address, farm_id))

    farm_transactions = farm_client.getTransactionsOfClosedPositions(market_id)
    farm_transactions_for_positions = farm_client.getFarmTransactionsForPositions(farm_address, farm_transactions, positions)
    farm_transactions_with_prices = farm_client.addRewardValueInUSD(farm_transactions_for_positions)

    return farm_transactions_with_prices

def collect_data_for_dai_weth():
    collect_data_for_pool(DAI_WETH_POOL, "stats/dai-eth.csv")

def collect_data_for_ldo_weth():
    collect_data_for_pool(LDO_WETH_POOL, "stats/ldo-weth.csv")

def collect_data_for_aave_weth():
    collect_data_for_pool(AAVE_WETH_POOL, "stats/aave-weth.csv")

def collect_data_for_usdc_weth():
    collect_data_for_pool(USDC_WETH_POOL, "stats/usdc-weth.csv")

def collect_data_for_wbtc_weth():
    collect_data_for_pool(WBTC_WETH_POOL, "stats/wbtc-weth.csv")

def collect_data_for_usdt_weth():
    collect_data_for_pool(USDT_WETH_POOL, "stats/usdt-weth.csv")

def collect_data_for_yfi_weth():
    collect_data_for_pool(YFI_WETH_POOL, "stats/yfi-weth.csv")

def profitability_ratio(filename):
    profitable = 0
    non_profitable = 0
    net_gain_profitable = 0
    net_gain_non_profitable = 0
    with open(filename, mode='r') as csv_file:
        stats = csv.DictReader(csv_file)

        for row in stats:
            if float(row['pool_vs_hodl_roi']) > 0:
                profitable += 1
            else:
                non_profitable += 1

            if float(row['pool_roi']) > 0:
                net_gain_profitable += 1
            else:
                net_gain_non_profitable += 1

        net_percentage = net_gain_profitable/(net_gain_profitable+net_gain_non_profitable)
        print("Net gain profitable: {} positions ({:.2f}%)".format(net_gain_profitable, round(net_percentage * 100, 2)))

        pool_vs_hodl_profitable_ratio = profitable / (profitable+non_profitable)
        print("Profitable compared to HODL strategy: {} positions ({:.2f}%)".format(profitable, round(pool_vs_hodl_profitable_ratio * 100, 2)))

def main():
    # collect_data_for_dai_weth()
    collect_data_for_ldo_weth()
    # collect_data_for_aave_weth()
    # collect_data_for_usdc_weth()
    # collect_data_for_wbtc_weth()
    # collect_data_for_usdt_weth()
    # collect_data_for_yfi_weth()

    # profitability_ratio("stats/dai-eth.csv")
    # profitability_ratio("stats/ldo-weth.csv")
    # profitability_ratio("stats/aave-weth.csv")
    # profitability_ratio("stats/usdc-weth.csv")
    # profitability_ratio("stats/wbtc-weth.csv")
    # profitability_ratio("stats/usdt-weth.csv")
    # profitability_ratio("stats/yfi-weth.csv")

if __name__ == "__main__":
    main()