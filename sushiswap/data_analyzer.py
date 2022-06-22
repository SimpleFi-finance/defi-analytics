#%%
# Load position data
import pandas as pd
import matplotlib.pyplot as plt
df = pd.read_csv("stats/usdc-weth.csv")
df.head(10)

# %%
# Plot ratio of profitable/non-profitable positions compared to HODL
pool_vs_hodl_roi_profitability = df['pool_vs_hodl_roi'].agg(
    profitable_positions = lambda s: s.gt(0).sum(),
    non_profitable_positions = lambda s: s.lt(0).sum()
)

pool_vs_hodl_roi_profitability.plot(
    kind='pie',
    legend=True,
    labeldistance=None,
    ylabel='',
    labels=['Outperformed HODL', 'Underperformed HODL'],
    autopct='%1.1f%%',
    explode=[0.05, 0.05],
    colors = ['green', 'red'],
    shadow=True,
    title='USDC-WETH position profitability compared to HODL'
    ).legend(loc='lower right')
plt.show()


# %%
# Plot histogram for position closing times
df['block_ranges'] = (df['position_end_block']/1000000).astype(int)
df['is_profitable'] = df['pool_vs_hodl_roi'].apply(lambda x: 1 if x > 0 else 0)

agg_stats = df.groupby('block_ranges')['is_profitable'].agg(Total='count', Profitable='sum')
agg_stats.plot(
    kind='bar',
    title='Closed USDC-WETH positions per 1M block ranges'
)