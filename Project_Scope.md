### Scope
To facilitate trading via rithmic and projectX
- Both apis facilitate trading and data
- Futures trading only
- to allow multiple stratgies to connect or hotload at run time by using an in memory message bus.
- to enable full functionality of both apis


### Accounts
- Accounts will always be USD
- Margin is irrelevant, instead we will have a max trade size per account

### Orders
- We will support all order types, including bracket orders.


### Backtesting.
The backtest engine will bypass the api client and will instead run internally, only live strategies will use the api client.

### Feeds
We will support
Ticks,
Quotes,
Depth10,
Bars1s,
Bars1m,
Bars1h,
Bars1d,

All other feeds will be consolidated, more complex feeds can be added later, mbo etc.