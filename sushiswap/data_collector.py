import csv
from position_handler import PositionHandler

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
DAI_WETH_POOL = "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f"
LDO_WETH_POOL = "0xc558f600b34a5f69dd2f0d06cb8a88d829b7420a"
AAVE_WETH_POOL = "0xd75ea151a61d06868e31f8988d28dfe5e9df57b4"
USDC_WETH_POOL = "0x397ff1542f962076d0bfe58ea045ffa2d347aca0"


def collect_data_for_pool(pool, filename):
    position_handler = PositionHandler(SUSHISWAP_ENDPOINT)

    raw_positions = position_handler.getRawClosedPositions(pool)
    print("Positions loaded from subgraph: {0}".format(len(raw_positions)))

    merged_positions = position_handler.mergePositionsByHistory(raw_positions)
    print("Positions after merging histories: {0}".format(len(merged_positions)))

    filtered_positions = position_handler.filterOutPositionsWithMultipleInvestsOrRedeems(merged_positions)
    print("Positions after filtering out multi-invest or multi-redeem positions: {0}".format(len(filtered_positions)))

    print("Calculating profitability...")
    profitability_stats = position_handler.calculateProfitabilityOfPositions(filtered_positions)
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
    # collect_data_for_ldo_weth()
    # collect_data_for_aave_weth()
    # collect_data_for_usdc_weth()
    # profitability_ratio("stats/dai-eth.csv")
    # profitability_ratio("stats/ldo-weth.csv")
    # profitability_ratio("stats/aave-weth.csv")
    profitability_ratio("stats/usdc-weth.csv")

if __name__ == "__main__":
    main()