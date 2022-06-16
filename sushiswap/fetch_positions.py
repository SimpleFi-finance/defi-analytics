from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from price_helper import PriceProvider

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
transport = AIOHTTPTransport(url=SUSHISWAP_ENDPOINT)

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=120)

# Provide a GraphQL query
### accountAddress: \"0x0291eb432cb4a2613a7415018933e3db45bcd769\", 
### market: \"0x06da0fd433c1a5d7a4faa01111c044910a184553\", 
query = gql(
    """
    query ($lastID: ID) {
      positions(first: 1000, where: {accountAddress: \"0x0291eb432cb4a2613a7415018933e3db45bcd769\", id_gt: $lastID, closed: true}) {
        id
        accountAddress
        history {
            id
            transaction {
                id
                blockNumber
                inputTokenAmounts
                outputTokenAmount
                transactionType
            }
        }
      }
    }
    """
)

lastID = ""
all_positions = {}

print("------ READING POSITIONS ---------")

while True:
    vars = {"lastID": lastID}
    response = client.execute(query, variable_values=vars)
    if not response['positions']:
        break

    for position in response['positions']:
        all_positions[position['id']] = []
        for positionSnapshot in position['history']:
            tx = positionSnapshot['transaction']
            tx_data = []
            tx_data.append(tx['id'])
            tx_data.append(tx['inputTokenAmounts'])
            tx_data.append(tx['outputTokenAmount'])
            tx_data.append(tx['transactionType'])
            tx_data.append(tx['blockNumber'])
            all_positions[position['id']].append(tx_data)
       
    lastID = response['positions'][-1]['id']
    print("Processed : ", len(all_positions))
 
print("Number of total positions: ", len(all_positions))

print("------ MERGING POSITIONS ---------")

TX_TYPE = 3
merged_positions = {}
processed_positions = {}
i = 0
non = 0
skip = 0
for position_id in all_positions.keys():
    i += 1
    if i%1000 == 0:
        print(i)

    # don't process position which was already processed as part of position merging
    if position_id in processed_positions:
        continue

    first_tx = all_positions[position_id][0]
    last_tx = all_positions[position_id][-1]

    # first TX has to be of type INVEST
    if first_tx[TX_TYPE] != "INVEST":
        skip += 1
        continue

    # if last TX is REDEEM then position is completed
    if last_tx[TX_TYPE] == "REDEEM":
        merged_positions[position_id] = all_positions[position_id]
        processed_positions[position_id] = True
        continue

    # if last TX is TRANSFER_OUT then search for subsequent TXs to make position complete
    if last_tx[TX_TYPE] == "TRANSFER_OUT":
        expanded_position = all_positions[position_id]
        expanded_position_complete = False

        # ie. 5 for position '0x0000000000000d9054f605ca65a2647c2b521422-0xb84c45174bfc6b8f3eaecbae11dee63114f5c1b2-INVESTMENT-5'
        current_position_counter = int(position_id.split("-")[-1])
        while True:
            next_position_id = position_id.rsplit('-', 1)[0] + "-" + str(current_position_counter + 1)
            next_position = all_positions.get(next_position_id)

            # handle case when there's no next position
            if next_position == None:
                non += 1
                break

            # if last TX is not REDEEM then loop again to pick up and merge next position
            elif next_position[-1][TX_TYPE] != "REDEEM":
                current_position_counter = int(next_position_id.split("-")[-1])
                expanded_position = expanded_position + next_position
                processed_positions[next_position_id] = True
                continue

            # position has been completed
            else:
                expanded_position = expanded_position + next_position
                expanded_position_complete = True
                processed_positions[next_position_id] = True
                break

        # add completed position to list
        if expanded_position_complete:
            merged_positions[position_id] = expanded_position
            processed_positions[position_id] = True


print("Number of positions after merging histories: ", len(merged_positions))
print("Non: ", non)
print("Skip: ", skip, "\n")



print("------ EXTRACT TOKENS AND TOKEN AMOUNTS ---------")


price_provider = PriceProvider()
for pos_id in merged_positions.keys():
    txs = merged_positions[pos_id]

    print('#######')
    # print(txs)
    print('----')


    first_tx = txs[0]
    last_tx = txs[-1]

    position_start_block = int(first_tx[4])
    position_end_block = int(last_tx[4])

    tokenA = first_tx[1][0].split("|")[0]
    tokenA_amount_start = int(first_tx[1][0].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenA))
    tokenA_price_start = price_provider.getTokenPriceinUSD(tokenA, position_start_block)
    if(tokenA_price_start == None):
        continue

    tokenA_amount_end = int(last_tx[1][0].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenA))
    tokenA_price_end = price_provider.getTokenPriceinUSD(tokenA, position_end_block)
    if(tokenA_price_end == None):
        continue

    tokenB = first_tx[1][1].split("|")[0]
    tokenB_amount_start = int(first_tx[1][1].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenB))
    tokenB_price_start = price_provider.getTokenPriceinUSD(tokenB, position_start_block)
    if(tokenB_price_start == None):
        continue

    tokenB_amount_end = int(last_tx[1][1].split("|")[2]) * pow(10, (-1) * price_provider.decimals(tokenB))
    tokenB_price_end = price_provider.getTokenPriceinUSD(tokenB, position_end_block)
    if(tokenB_price_end == None):
        continue

    position_start_value = tokenA_amount_start * tokenA_price_start + tokenB_amount_start * tokenB_price_start
    position_end_value = tokenA_amount_end * tokenA_price_end + tokenB_amount_end * tokenB_price_end
    position_end_value_if_held = tokenA_amount_start * tokenA_price_end + tokenB_amount_start * tokenB_price_end

    print("User starts position with:")
    print("tokenA: {0}".format(tokenA))
    print("tokenA amount: {0:.2f}".format(tokenA_amount_start))
    print("tokenA price: ${0:.2f}".format(tokenA_price_start))

    print("tokenB: {0}".format(tokenB))
    print("tokenB amount: {0:.2f}".format(tokenB_amount_start))
    print("tokenB price: {0:.2f}".format(tokenB_price_start))
    print("Position value: ${0:.2f}\n".format(position_start_value))


    print("User ends position with:")
    print("tokenA amount: {0:.2f}".format(tokenA_amount_end))
    print("tokenA price: ${0:.2f}".format(tokenA_price_end))

    print("tokenB amount: {0:.2f}".format(tokenB_amount_end))
    print("tokenB price: {0:.2f}".format(tokenB_price_end))
    print("Position value: ${0:.2f}\n".format(position_end_value))

    pool_net_gain = position_end_value - position_start_value
    hodl_net_gain = position_end_value_if_held - position_start_value

    print("HODL position value : ${0:.2f}".format(position_end_value_if_held))
    print("Net gain investing in pool : ${0:.2f}".format(pool_net_gain))
    print("Net gain if HODLed : ${0:.2f}".format(hodl_net_gain))


    print('#######')



    # print("Position: ", pos_id)

    # print("TokenA = ", tokenA)
    # print("TokenA start = ", tokenA_amount_start)
    # print("TokenA end = ", tokenA_amount_end)

    # print("TokenB = ", tokenB)
    # print("TokenB start = ", tokenB_amount_start)
    # print("TokenB end = ", tokenB_amount_end)

    # print("Position start block = ", position_start_block)
    # print("Position end block = ", position_end_block)

    # print("\n")


