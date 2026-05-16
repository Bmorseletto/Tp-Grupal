A distributed system is required to analyze transaction records between bank accounts for anomalies.

- The system must be optimized for multi-computer environments.
- It must support the scaling of computing resources to handle the volume of data to be processed.

- Middleware development is required to abstract group-based communication.

- It must support single-run processing and provide graceful quit upon receiving SIGTERM signals.

- It must be developed using either python or golang.


The following should be obtained:
1. Source account, destination account, and amount for USD transactions under 50.

2. Bank name, source account, and maximum USD transaction amount for each bank.

3. Source account and USD transaction amount in the period [2022-09-06, 2022-09-15]
with an amount less than one hundredth of the average found for the same payment method in the period [2022-09-01, 2022-09-05]
4. Accounts that meet the scatter-gather pattern with a single separating account,
for accounts that have made and whose source account has made USD transfers to between 5 different accounts within the period [2022-09-01, 2022-09-05]
5. Number of transactions in the period [2022-09-01, 2022-09-05] with the payment method "Wire" or "ACH" whose amount converted to USD is less than 1