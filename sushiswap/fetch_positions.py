from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

SUSHISWAP_ENDPOINT = "https://api.thegraph.com/subgraphs/name/simplefi-finance/sushiswap"
transport = AIOHTTPTransport(url=SUSHISWAP_ENDPOINT)

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

# Provide a GraphQL query
### accountAddress: \"0x0291eb432cb4a2613a7415018933e3db45bcd769\", 
### market: \"0x06da0fd433c1a5d7a4faa01111c044910a184553\", 
query = gql(
    """
    query ($lastID: ID) {
      positions(first: 1000, where: {id_gt: $lastID, closed: true}) {
        id
        closed
        history {
            id
            transaction {
                id
                inputTokenAmounts
                outputTokenAmount
                transactionType
                transferredTo
            }
        }
      }
    }
    """
)

lastID = ""
all_positions = []

print("------ READING POSITIONS ---------")

while True:
    vars = {"lastID": lastID}
    response = client.execute(query, variable_values=vars)
    if not response['positions']:
        break

    all_positions += response['positions']
    lastID = response['positions'][-1]['id']
    print(len(all_positions))



merged_positions = []
print(len(all_positions))


print("------ MERGING POSITIONS ---------")

for pos in all_positions:
    if pos['history'][0]['transaction']['transactionType'] == "INVEST":
        continue
    
    if pos['history'][-1]['transaction']['transactionType'] == "REDEEM":
        merged_positions.append(pos)
        continue

    if pos['history'][-1]['transaction']['transactionType'] == "TRANSFER_OUT":
        current_position_counter = int(pos['id'].split("-")[-1])
        next_position_id = pos['id'][0:-1] + str(current_position_counter + 1)

        next_position = next((p for p in all_positions if p['id'] == next_position_id), None)
        if next_position == None or next_position['closed'] == False:
            continue

        pos['history'] += next_position['history']
        merged_positions.append(pos)
        continue

print(len(merged_positions))



