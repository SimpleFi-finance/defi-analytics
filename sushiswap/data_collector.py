from position_handler import PositionHandler
from graph_clients import SushiswapFarmsClient
import os.path

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
SUSHISWAP_FARMS_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap-farms"

pools = {}
pools["DAI_WETH"] = "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f"
pools['LDO_WETH']= "0xc558f600b34a5f69dd2f0d06cb8a88d829b7420a"
pools['AAVE_WETH'] = "0xd75ea151a61d06868e31f8988d28dfe5e9df57b4"
pools['USDC_WETH'] = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"
pools['WBTC_WETH'] = "0xceff51756c56ceffca006cd410b03ffc46dd3a58"
pools['USDT_WETH'] = "0x06da0fd433c1a5d7a4faa01111c044910a184553"
pools['YFI_WETH'] = "0x088ee5007c98a9677165d78dd2109ae4a3d04d0c"
pools['ILV_WETH'] = "0x6a091a3406e0073c3cd6340122143009adac0eda"
pools['TOKE_WETH'] = "0xd4e7a6e2d03e4e48dfc27dd3f46df1c176647e38"
pools['SUSHI_WETH'] = "0x795065dcc9f64b5614c407a6efdc400da6221fb0"
pools['BIT_WETH'] = "0xe12af1218b4e9272e9628d7c7dc6354d137d024e"
pools['ALCX_WETH'] = "0xc3f279090a47e80990fe3a9c30d24cb117ef91a8"
pools['PUNK_WETH'] = "0x0463a06fbc8bf28b3f120cd1bfc59483f099d332"
pools['YGG_WETH'] = "0x99b42f2b49c395d2a77d973f6009abb5d67da343"
pools['JPEG_WETH'] = "0xdb06a76733528761eda47d356647297bc35a98bd"
pools['SRM_WETH'] = "0x117d4288b3635021a3d612fe05a3cbf5c717fef2"
pools['COMP_WETH'] = "0x31503dcb60119a812fee820bb7042752019f2355"
pools['OHM_DAI'] = "0x055475920a8c93cffb64d039a8205f7acc7722d3"
pools['OHM_WETH'] = "0x69b81152c5a8d35a67b32a4d3772795d96cae4da"
pools['WXRP_WETH'] = "0xa7a8edfda2b8bf1e5084e2765811effee21ef918"
pools['CRV_WETH'] = "0x58dc5a51fe44589beb22e8ce67720b5bc5378009"
pools['RADAR_WETH'] = "0x559ebe4e206e6b4d50e9bd3008cda7ce640c52cb"
pools['METIS_WETH'] = "0xdab6d56915d36060c8d6cf29a7a84910da614603"
pools['YGG_WETH'] = "0x99b42f2b49c395d2a77d973f6009abb5d67da343"
pools['KP3R_WETH'] = "0xaf988aff99d3d0cb870812c325c588d8d8cb7de8"
pools['UST_WETH'] = "0x8b00ee8606cc70c2dce68dea0cefe632cca0fb7b"
pools['SPELL_WETH'] = "0xb5de0c3753b6e1b4dba616db82767f17513e6d4e"
pools['BOND_WETH'] = "0x613c836df6695c10f0f4900528b6931441ac5d5a"
pools['REN_WETH'] = "0x611cde65dea90918c0078ac0400a72b0d25b9bb1"
pools['CREAM_WETH'] = "0xf169cea51eb51774cf107c88309717dda20be167"
pools['CVX_WETH'] = "0x05767d9ef41dc40689678ffca0608878fb3de906"
pools['SNX_WETH'] = "0xa1d7b2d891e3a1f9ef4bbc5be20630c2feb1c470"
pools['MKR_WETH'] = "0xba13afecda9beb75de5c56bbaf696b880a5a50dd"
pools['UNI_WETH'] = "0xdafd66636e2561b0284edde37e42d192f2844d40"


def collect_data_for(pool, filename):
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


def collect_data_for_alls():
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
    if market_id == None:
        return []
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

def collect_top_20_by_tvl():
    protocols = [
        'ILV_WETH',
        'USDC_WETH',
        'OHM_DAI',
        'USDT_WETH',
        'WBTC_WETH',
        'OHM_WETH',
        'SUSHI_WETH',
        'BIT_WETH',
        'TOKE_WETH',
        'ALCX_WETH',
        'AAVE_WETH',
        'DAI_WETH',
        'PUNK_WETH',
        'WXRP_WETH',
        'YFI_WETH',
    ]

    for p in protocols:
        print("\nCollecting data for " + p + "...")
        filename = "top20-tvl-stats/" + p + ".csv"

        # if data exists don't collect it again
        if os.path.isfile(filename):
            continue

        collect_data_for(pools[p], filename)

def collect_top_20_by_trading_volume():
    protocols = [
        'CRV_WETH',
        'ALCX_WETH',
        'RADAR_WETH',
        'METIS_WETH',
        'OHM_WETH',
        'DAI_WETH',
        'SUSHI_WETH',
        'YGG_WETH',
        'KP3R_WETH',
        'YFI_WETH',
        'UST_WETH',
        'WBTC_WETH',
        'SPELL_WETH',
        'BOND_WETH',
        'AAVE_WETH',
        'REN_WETH',
        'CREAM_WETH',
        'COMP_WETH'
    ]

    for p in protocols:
        print("\nCollecting data for " + p + "...")
        filename = "top20-volume-stats/" + p + ".csv"

        # if data exists don't collect it again
        if os.path.isfile(filename):
            continue

        collect_data_for(pools[p], filename)


def collect_defi_pools():
    protocols = [
        'CRV_WETH',
        'SUSHI_WETH',
        'YFI_WETH',
        'AAVE_WETH',
        'COMP_WETH',
        'CVX_WETH',
        'SNX_WETH',
        'MKR_WETH',
        'UNI_WETH'
    ]

    for p in protocols:
        print("\nCollecting data for " + p + "...")
        filename = "defi-pools-stats/" + p + ".csv"

        # if data exists don't collect it again
        if os.path.isfile(filename):
            continue

        collect_data_for(pools[p], filename) 


def collect_stablecoin_pools():
    protocols = [
        'USDC_WETH',
        'USDT_WETH',
        'DAI_WETH',
    ]

    for p in protocols:
        print("\nCollecting data for " + p + "...")
        filename = "stablecoin-pools-stats/" + p + ".csv"

        # if data exists don't collect it again
        if os.path.isfile(filename):
            continue

        collect_data_for(pools[p], filename) 


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
    # collect_data_for_ygg_weth()
    # collect_data_for_jpeg_weth()
    # collect_data_for_srm_weth()
    # collect_data_for_comp_weth()
    # collect_data_for_punk_weth()

    # collect_top_20_by_tvl()
    # collect_top_20_by_trading_volume()
    collect_defi_pools()
    # collect_stablecoin_pools()

    # collect_data_for_alls()

if __name__ == "__main__":
    main()