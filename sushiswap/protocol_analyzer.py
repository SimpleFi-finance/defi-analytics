#%%
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
pd.set_option('display.float_format', '{:.2f}'.format)
net_gains = df[['market', 'pool_vs_hodl_roi']]
net_gains_mean = net_gains.groupby(['market']).mean()
ax = net_gains_mean.plot.bar(rot=90)

ax.set_title('Average pool_vs_hodl_roi per pool', fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both')
# ax.set_xticklabels(net_gains['market'], rotation=90, ha='right')
ax


# %%
# FILE_NAME = "usdc-weth.csv"

# usdc_weth_df = pd.read_csv("stats/" + FILE_NAME, parse_dates=["position_end_date", "position_start_date"])
# usdc_weth_df = usdc_weth_df[usdc_weth_df["position_investment_value"] > 100]
# usdc_weth_df = usdc_weth_df[usdc_weth_df["position_redemption_value"] > 100] 
# usdc_weth_df = usdc_weth_df[usdc_weth_df["position_start_date"] != usdc_weth_df["position_end_date"]] 
# usdc_weth_df.sort_values(by='pool_net_gain').head(10)

# %%
