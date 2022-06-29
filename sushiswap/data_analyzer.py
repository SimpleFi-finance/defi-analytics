#%%
# Load position data
import pandas as pd
df = pd.read_csv("stats/usdc-weth.csv")
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
ax.set_title('LDO-WETH position profitability', fontweight='bold', color= 'yellow');
ax

# %%
# Plot histogram for position closing times
def get_h_for_block(block):
    if block >= 9193266 and block < 10370274:
        return '2020 H1'
    elif block >= 10370274 and block < 11565019:
        return '2020 H2'
    if block >= 11565019 and block < 12738509:
        return '2021 H1'
    elif block >= 12738509 and block < 13916166:
        return '2021 H2'
    elif block >= 13916166:
        # TODO update when H1 is finished
        return '2022 H1'

df['time_ranges'] = df['position_end_block'].apply(lambda x: get_h_for_block(x)).sort_values()
df['is_profitable'] = df['pool_vs_hodl_roi'].apply(lambda x: 1 if x > 0 else 0)

agg_stats = df.groupby('time_ranges')['is_profitable'].agg(Total='count', Profitable='sum')
ax = agg_stats.plot(
    kind='bar',
)
ax.set_title('Number of closed positions in LDO-WETH', fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both', rotation='auto')
ax

# %%
# Describe returns
def remove_outliers(df, column_name):
    q_low = df[column_name].quantile(0.01)
    q_hi  = df[column_name].quantile(0.95)
    return df[(df[column_name] < q_hi) & (df[column_name] > q_low)]

filtered_df = remove_outliers(df, 'pool_roi')
pd.set_option('display.float_format', '{:.6f}'.format)
filtered_df["pool_roi"].describe()

# filtered_df['pool_roi'].describe()
filtered_df["pool_roi"].plot(kind='hist', bins=100)

# %%
