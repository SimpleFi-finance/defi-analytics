#%%
# Load position data
import pandas as pd

DATASET_NAME = "TOP20-TVL"
FILE_NAME = "top20-tvl-stats/top20-tvl-combined.csv"

df = pd.read_csv(FILE_NAME, parse_dates=["position_end_date", "position_start_date"])
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
ax.set_title(DATASET_NAME + ' profitability vs HODL', fontweight='bold', color= 'yellow');
ax


# %%
# Plot ratio of profitable/non-profitable positions vs HODL including rewards
df['total_roi_vs_hodl'] = df['pool_vs_hodl_roi'] + df['claimed_rewards_in_USD']/df['position_redemption_value_if_held']

total_profitability = df['total_roi_vs_hodl'].agg(
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
ax.set_title(DATASET_NAME + ' profitability vs HODL including rewards', fontweight='bold', color= 'yellow');
ax


# %%
# Plot ratio of profitable/non-profitable positions vs USD
pool_vs_usd_roi_profitability = df['pool_roi'].agg(
    profitable_positions = lambda s: s.gt(0).sum(),
    non_profitable_positions = lambda s: s.lt(0).sum()
)

ax = pool_vs_usd_roi_profitability.plot(
    kind='pie',
    legend=True,
    labeldistance=None,
    ylabel='',
    labels=['Outperformed USD', 'Underperformed USD'],
    autopct='%1.1f%%',
    explode=[0.05, 0.05],
    colors = ['green', 'red'],
    shadow=True,
    )
ax.legend(loc='lower right')
ax.set_title(DATASET_NAME + ' profitability vs USD', fontweight='bold', color= 'yellow');
ax

# %%
# Plot ratio of profitable/non-profitable positions vs USD including rewards
df['total_pool_net_gain'] = df['pool_net_gain'] + df['claimed_rewards_in_USD']

total_profitability = df['total_pool_net_gain'].agg(
    profitable_positions = lambda s: s.gt(0).sum(),
    non_profitable_positions = lambda s: s.lt(0).sum()
)

ax = total_profitability.plot(
    kind='pie',
    legend=True,
    labeldistance=None,
    ylabel='',
    labels=['Outperformed USD', 'Underperformed USD'],
    autopct='%1.1f%%',
    explode=[0.05, 0.05],
    colors = ['green', 'red'],
    shadow=True,
    )
ax.legend(loc='lower right')
ax.set_title(DATASET_NAME + ' profitability vs USD including rewards', fontweight='bold', color= 'yellow');
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
    elif block >= 13916166 and block < 15053287:
        # TODO update when H1 is finished
        return '2022 H1'

df['time_ranges'] = df['position_end_block'].apply(lambda x: get_h_for_block(x)).sort_values()
df['is_profitable'] = df['pool_vs_hodl_roi'].apply(lambda x: 1 if x > 0 else 0)

agg_stats = df.groupby('time_ranges')['is_profitable'].agg(Total='count', Profitable='sum')
ax = agg_stats.plot(
    kind='bar',
)
ax.set_title('Number of closed positions in ' + DATASET_NAME, fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both', rotation='auto')
ax


# %%
# Plot winners, losers and inbetweeners
df['time_ranges'] = df['position_end_block'].apply(lambda x: get_h_for_block(x)).sort_values()
df['profitability_group'] = df['total_roi_vs_hodl'].apply(
    lambda x: 'winner' if x > 1 else ('loser' if x < -0.6 else 'inbetweener'))

groups = df.groupby('profitability_group')['profitability_group'].count()

ax = groups.plot(
    kind='pie',
    legend=True,
    labeldistance=None,
    ylabel='',
    labels=['inbetween', 'ROI < -60%', 'ROI > 100%'],
    autopct='%1.1f%%',
    explode=[0.05, 0.05, 0.05],
    colors = ['pink', 'red', 'green'],
    shadow=True,
    )
ax.legend(loc='lower right')
ax.set_title(DATASET_NAME + ' performance groups', fontweight='bold', color= 'yellow');
ax


# %%
# Describe returns by plotting ROI distribution
def remove_outliers(df, column_name):
    q_low = df[column_name].quantile(0.01)
    q_hi  = df[column_name].quantile(0.95)
    return df[(df[column_name] < q_hi) & (df[column_name] > q_low)]

filtered_df = remove_outliers(df, 'pool_roi')
pd.set_option('display.float_format', '{:.6f}'.format)

ax = filtered_df["pool_roi"].plot(kind='hist', bins=100)
ax.set_title('Distribution of returns for ' + DATASET_NAME, fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both', rotation='auto')
ax

# %%
# Plot correlation between pool ROIs and position closing date
filtered_df.sort_values(by=['position_end_date'], inplace=True)
ax = filtered_df.plot.scatter(x='position_end_date', y='pool_roi', s=5)
ax.set_title('Scatter position closing dates ' + DATASET_NAME, fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both', rotation='auto')
num_of_ticks = round(len(filtered_df['position_end_date'].index)/10)
labels = filtered_df['position_end_date'][::num_of_ticks]
ax.set_xticklabels(labels.dt.date, rotation=30, ha='right')
ax

# %%
# Plot correlation between pool ROIs and position opening date
filtered_df.sort_values(by=['position_start_date'], inplace=True)
ax = filtered_df.plot.scatter(x='position_start_date', y='pool_roi', s=5)
ax.set_title('Scatter position opening dates ' + DATASET_NAME, fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both', rotation='auto')
labels = filtered_df['position_start_date'][::num_of_ticks]
ax.set_xticklabels(labels.dt.date, rotation=30, ha='right')
ax

# %%
# Plot correlation between pool ROIs and position_duration
filtered_df['position_duration'] = (filtered_df['position_end_date'] - filtered_df['position_start_date']).dt.days
filtered_df.sort_values(by=['position_duration'], inplace=True)
ax = filtered_df.plot.scatter(x='position_duration', y='pool_roi', s=5)
ax.set_title('Scatter position duration vs pool ROI ' + DATASET_NAME, fontweight='bold', color= 'yellow');
ax.tick_params(colors='yellow', which='both', rotation='auto')
ax

# %%
# Get top 20 most profitable accounts
df_by_account = df.groupby('account')['total_pool_net_gain'].sum().reset_index(name ='total_pool_net_gain')
df_by_account.sort_values(by=['total_pool_net_gain'], inplace=True)
df_by_account

# %%
