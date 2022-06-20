from curses import raw
from position_handler import PositionHandler

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"

def main():
    position_handler = PositionHandler(SUSHISWAP_ENDPOINT)

    raw_positions = position_handler.getRawClosedPositions()
    print("Positions loaded from subgraph: {0}".format(len(raw_positions)))

    merged_positions = position_handler.mergePositionsByHistory(raw_positions)
    print("Positions after merging histories: {0}".format(len(merged_positions)))

    filtered_positions = position_handler.filterOutPositionsWithMultipleInvestsOrRedeems(merged_positions)
    print("Positions after filtering out multi-invest or multi-redeem positions: {0}".format(len(filtered_positions)))

    profitability_stats = position_handler.calculateProfitabilityOfPositions(filtered_positions)
    print("Profitability stats ready")

    filename = "stats/dai-eth.csv"
    position_handler.writeProfitabilityStatsToCsv(profitability_stats, filename)
    print("Stats written to {0}".format(filename))


if __name__ == "__main__":
    main()