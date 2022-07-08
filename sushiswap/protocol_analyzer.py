#%%
# Make name mappings
pairs = {}
pairs["0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f"] = "DAI_WETH"
pairs["0xc558f600b34a5f69dd2f0d06cb8a88d829b7420a"] = "LDO_WETH"
pairs["0xd75ea151a61d06868e31f8988d28dfe5e9df57b4"] = "AAVE_WETH"
pairs["0x397ff1542f962076d0bfe58ea045ffa2d347aca0"] = "USDC_WETH"
pairs["0xceff51756c56ceffca006cd410b03ffc46dd3a58"] = "WBTC_WETH"
pairs["0x06da0fd433c1a5d7a4faa01111c044910a184553"] = "USDT_WETH"
pairs["0x088ee5007c98a9677165d78dd2109ae4a3d04d0c"] = "YFI_WETH_"
pairs["0x6a091a3406e0073c3cd6340122143009adac0eda"] = "ILV_WETH_"
pairs["0xd4e7a6e2d03e4e48dfc27dd3f46df1c176647e38"] = "TOKE_WETH"
pairs["0x795065dcc9f64b5614c407a6efdc400da6221fb0"] = "SUSHI_WETH"
pairs["0xe12af1218b4e9272e9628d7c7dc6354d137d024e"] = "BIT_WETH"
pairs["0xc3f279090a47e80990fe3a9c30d24cb117ef91a8"] = "ALCX_WETH"
pairs["0x0463a06fbc8bf28b3f120cd1bfc59483f099d332"] = "PUNK_WETH"
pairs["0x4a86c01d67965f8cb3d0aaa2c655705e64097c31"] = "SYN_WETH"
pairs["0x99b42f2b49c395d2a77d973f6009abb5d67da343"] = "YGG_WETH"
pairs["0xdb06a76733528761eda47d356647297bc35a98bd"] = "JPEG_WETH"
pairs["0x117d4288b3635021a3d612fe05a3cbf5c717fef2"] = "SRM_WETH"
pairs["0x31503dcb60119a812fee820bb7042752019f2355"] = "COMP_WETH"

# Load position data
import pandas as pd

FILE_NAME = "combined.csv"

df = pd.read_csv("stats/" + FILE_NAME, parse_dates=["position_end_date", "position_start_date"])
df = df[df["position_investment_value"] > 100]
df = df[df["position_redemption_value"] > 100] 

df.head(10)

# %%
# Plot ratio of profitable/non-profitable positions compared to HODL
pool_vs_hodl_roi_profitability = df['pool_vs_hodl_roi'].agg(
    profitable_positions = lambda s: s.gt(0).sum(),
    non_profitable_positions = lambda s: s.lt(0).sum()
)

ax = pool_vs_hodl_roi_profitability.plot(
    kind='pie',
    legend=True,
    labeldistance=None,
    ylabel='',
    labels=['Outperformed HODL', 'Underperformed HODL'],
    autopct='%1.1f%%',
    explode=[0.05, 0.05],
    colors = ['green', 'red'],
    shadow=True,
    )
ax.legend(loc='lower right')
ax.set_title('Sushiswap position profitability', fontweight='bold', color= 'yellow');
ax

# %%
# Plot ratio of profitable/non-profitable positions, including rewards, compared to HODL

df['total_roi'] = df['pool_net_gain'] + df['claimed_rewards_in_USD']
df['total_roi_vs_hodl_roi'] = df['pool_vs_hodl_roi'] + df['claimed_rewards_in_USD']/df['position_redemption_value_if_held']

total_profitability = df['total_roi_vs_hodl_roi'].agg(
    profitable_positions = lambda s: s.gt(0).sum(),
    non_profitable_positions = lambda s: s.lt(0).sum()
)

ax = total_profitability.plot(
    kind='pie',
    legend=True,
    labeldistance=None,
    ylabel='',
    labels=['Outperformed HODL', 'Underperformed HODL'],
    autopct='%1.1f%%',
    explode=[0.05, 0.05],
    colors = ['green', 'red'],
    shadow=True,
    )
ax.legend(loc='lower right')
ax.set_title('Sushiswap position profitability including rewards', fontweight='bold', color= 'yellow');
ax

# %%
# Plot bar chart of mean pool_vs_hodl_roi per pool
pd.set_option('display.float_format', '{:.2f}'.format)
pool_vs_hodl = df[['market', 'pool_vs_hodl_roi']]
pool_vs_hodl_mean = pool_vs_hodl.groupby(['market']).mean()
ax = pool_vs_hodl_mean.plot.bar(rot=90)

ax.set_title('Average pool_vs_hodl_roi per pool', fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both')

pool_vs_hodl_labels = [pairs[pair_address.get_text()] for pair_address in ax.get_xticklabels()]
ax.set_xticklabels(pool_vs_hodl_labels, rotation=60, ha='right')
ax

# %%
# Plot bar chart of mean pool_net_gain per pool
net_gains = df[['market', 'pool_net_gain']]
net_gains_mean = net_gains.groupby(['market']).mean()
ax = net_gains_mean.plot.bar(rot=90)

ax.set_title('Average net gain per pool', fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both')

net_gain_labels = [pairs[pair_address.get_text()] for pair_address in ax.get_xticklabels()]
ax.set_xticklabels(net_gain_labels, rotation=60, ha='right')
ax


# %%
