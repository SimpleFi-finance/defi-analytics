from curses import raw
from position_handler import PositionHandler

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
DAI_WETH_POOL = "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f"
LDO_WETH_POOL = "0xc558f600b34a5f69dd2f0d06cb8a88d829b7420a"

def analyze_pool(pool, filename):
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

def analyze_dai_weth_pool():
    analyze_pool(DAI_WETH_POOL, "stats/usdc-eth.csv")

def analyze_ldo_weth_pool():
    analyze_pool(LDO_WETH_POOL, "stats/ldo-weth.csv")

def main():
    #analyze_dai_weth_pool()
    analyze_ldo_weth_pool()



if __name__ == "__main__":
    main()