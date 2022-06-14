### Sushiswap LP Position profitability

User initially holds 2 assets, ie. 10 ETH and 1000 AAVE. He has 2 options:

- invest - provide liquidity to Sushi pool and get SLP tokens
  - later when user decides to close position he burns SLP tokens and gets back input tokens
  - amounts of redeemed input tokens are different compared to initial amounts due to IL - user will get more of worse performing asset
  - amounts of redeemed input tokens involve trading fees earned (0.25% in case of Sushiswap)
  - user also gets reward tokens - Sushi and potentially additional reward token
  - ie. ETH outperformed AAVE during the period of investment -> user redeems 7 ETH, 1200 AAVE + 300 SUSHI reward tokens
- HODL - user keeps holding his 2 assets, doesn't invest them anywhere

Over the investing period, we can calculate:

- Net profit/loss of 'invest' -> USD(7 ETH, 1200 AAVE + 300 SUSHI) - USD value at invest time of (10 ETH, 1000 AAVE)
- Net profit/loss of 'HODL' -> USD(10 ETH, 1000 AAVE) - USD value at invest time of (10 ETH, 1000 AAVE)
- ROI for both options
- Net profit/loss of 'invest' vs. 'HODL'
- ROI diff of 'invest' vs. 'HODL'

### Analytics ideas

- profitable/unprofitable positions (total, ratio)

  - per pool
  - per chain
  - per protocol (Uni forks)

- correlation between profitability and

  - position duration
  - position number of trades
  - position size
  - pool trading volumes
  - pool TVL
  - pool V/R ratio
  - pool underlying assets
  - pool underlying assets price divergence (IL)

- IL vs trading fees vs reward tokens
  - change in number of profitable positions if there were no rewards
  - change in number of profitable positions if there was no trading fees
  - impact of Sushi rewards vs additional token rewards
