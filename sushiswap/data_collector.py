import csv
from position_handler import PositionHandler
from graph_clients import SushiswapFarmsClient

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
SUSHISWAP_FARMS_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap-farms"

DAI_WETH_POOL = "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f"
LDO_WETH_POOL = "0xc558f600b34a5f69dd2f0d06cb8a88d829b7420a"
AAVE_WETH_POOL = "0xd75ea151a61d06868e31f8988d28dfe5e9df57b4"
USDC_WETH_POOL = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"
WBTC_WETH_POOL = "0xceff51756c56ceffca006cd410b03ffc46dd3a58"
USDT_WETH_POOL = "0x06da0fd433c1a5d7a4faa01111c044910a184553"
YFI_WETH_POOL = "0x088ee5007c98a9677165d78dd2109ae4a3d04d0c"
ILV_WETH_POOL = "0x6a091a3406e0073c3cd6340122143009adac0eda"
TOKE_WETH_POOL = "0xd4e7a6e2d03e4e48dfc27dd3f46df1c176647e38"
SUSHI_WETH_POOL = "0x795065dcc9f64b5614c407a6efdc400da6221fb0"
BIT_WETH_POOL = "0xe12af1218b4e9272e9628d7c7dc6354d137d024e"
ALCX_WETH_POOL = "0xc3f279090a47e80990fe3a9c30d24cb117ef91a8"

PUNK_WETH_POOL = "0x0463a06fbc8bf28b3f120cd1bfc59483f099d332"
SYN_WETH_POOL = "0x4a86c01d67965f8cb3d0aaa2c655705e64097c31"
FXS_WETH_POOL = "0x61eb53ee427ab4e007d78a9134aacb3101a2dc23"

YGG_WETH_POOL = "0x99b42f2b49c395d2a77d973f6009abb5d67da343"
JPEG_WETH_POOL = "0xdb06a76733528761eda47d356647297bc35a98bd"
SRM_WETH_POOL = "0x117d4288b3635021a3d612fe05a3cbf5c717fef2"
COMP_WETH_POOL = "0x31503dcb60119a812fee820bb7042752019f2355"


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
    profitability_stats = position_handler.calculateProfitabilityOfPoolPositions(merged_positions, farm_transactions)
    print("Profitability stats ready")

    position_handler.writeProfitabilityStatsToCsv(profitability_stats, filename)
    print("Stats written to {0}".format(filename))



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

def collect_data_for_ilv_weth():
    collect_data_for_pool(ILV_WETH_POOL, "stats/ilv-weth.csv")

def collect_data_for_toke_weth():
    collect_data_for_pool(TOKE_WETH_POOL, "stats/toke-weth.csv")

def collect_data_for_sushi_weth():
    collect_data_for_pool(SUSHI_WETH_POOL, "stats/sushi-weth.csv")

def collect_data_for_bit_weth():
    collect_data_for_pool(BIT_WETH_POOL, "stats/bit-weth.csv")

def collect_data_for_alcx_weth():
    collect_data_for_pool(ALCX_WETH_POOL, "stats/alcx-weth.csv")

def collect_data_for_punk_weth():
    collect_data_for_pool(PUNK_WETH_POOL, "stats/punk-weth.csv")

def collect_data_for_syn_weth():
    collect_data_for_pool(SYN_WETH_POOL, "stats/syn-weth.csv")

def collect_data_for_fxs_weth():
    collect_data_for_pool(FXS_WETH_POOL, "stats/fxs-weth.csv")

def collect_data_for_ygg_weth():
    collect_data_for_pool(YGG_WETH_POOL, "stats/ygg-weth.csv")

def collect_data_for_jpeg_weth():
    collect_data_for_pool(JPEG_WETH_POOL, "stats/jpeg-weth.csv")

def collect_data_for_srm_weth():
    collect_data_for_pool(SRM_WETH_POOL, "stats/srm-weth.csv")

def collect_data_for_comp_weth():
    collect_data_for_pool(COMP_WETH_POOL, "stats/comp-weth.csv")

def collect_data_for_all_pools():
    position_handler = PositionHandler(SUSHISWAP_ENDPOINT)

    raw_positions = position_handler.getAllRawClosedPositions()
    print("Positions loaded from subgraph: {0}".format(len(raw_positions)))

    merged_positions = position_handler.mergePositionsByHistory(raw_positions)
    print("Positions after merging histories: {0}".format(len(merged_positions)))

    farm_transactions = collect_data_for_all_farms(merged_positions)
    print("Farm transactions for merged positions: {0}".format(len(farm_transactions)))

    print("Calculating profitability...")
    profitability_stats = position_handler.calculateProfitabilityOfAllPositions(merged_positions, farm_transactions)
    print("Profitability stats ready")

    filename = "stats/all-positions.csv"
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

def collect_data_for_all_farms(positions):
    farm_client = SushiswapFarmsClient()
    
    farms = farm_client.getAllMarkets()
    farm_transactions = farm_client.getTransactionsOfAllClosedPositions()
    farm_transactions_for_positions = farm_client.getFarmTransactionsForAllPositions(farms, farm_transactions, positions)
    farm_transactions_with_prices = farm_client.addRewardValueInUSD(farm_transactions_for_positions)

    return farm_transactions_with_prices

def main():
    # collect_data_for_dai_weth()
    # collect_data_for_ldo_weth()
    # collect_data_for_aave_weth()
    # collect_data_for_usdc_weth()
    # collect_data_for_wbtc_weth()
    # collect_data_for_usdt_weth()
    # collect_data_for_yfi_weth()
    # collect_data_for_ilv_weth()

    # collect_data_for_sushi_weth()
    # collect_data_for_toke_weth()
    # collect_data_for_bit_weth()
    # collect_data_for_alcx_weth()
    collect_data_for_syn_weth()
    collect_data_for_ygg_weth()
    collect_data_for_jpeg_weth()
    collect_data_for_srm_weth()
    collect_data_for_comp_weth()
    collect_data_for_punk_weth()
    
    # collect_data_for_all_pools()

if __name__ == "__main__":
    main()