Filter 1: Convert amount to USD -> Filter with < 50 USD
Filter 2: Convert amount to USD -> Group by bank and extract top 1 max. Transaction per bank
Filter 3: Convert amount to USD -> Filter by period [2022-09-01, 2022-09-05] -> average calculation

Convert amount to USD -> Filter by period [2022-09-06, 2022-09-15] -> average calculation

The results of both go to another filter, and the average is multiplied by 1/100, and a < filter is applied for comparison.
Filter 4: Filter by time period -> group accounts and transactions ({C1: [T1, T2, T3, ...]} -> groups initial accounts with final accounts ([(C1, Cn)]).
Filter 5: Convert amount to USD -> Filter by time period -> filter by payment format -> filter for amount < 1 USD

Architecture:
client --> gateway --> exchange(direct) --> usd converter --> filter 1
                                                          |
                                                           -> filter 2
                                                          |
                                                           -> filter 3 -> filter 4.2
                                                          |
                                                           -> filter 4 -> filter 4.2 -> filter 4.3
                                                          |
                                                           -> filter 5