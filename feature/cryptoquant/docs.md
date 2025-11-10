Exchange Inflow CDD (Coin Days Destroyed)

Exchange Inflow CDD is a subset of Coin Days Destroyed (CDD) where coins are destroyed by flowing into exchanges. This indicator is noise-removed version of CDD with respect to exchange dumping signal.
Authorizations:
Access Token
HTTP: Access Token

For each API request, include this HTTP header: Authorization with the Bearer {access_token}. Bearer access token is the type of HTTP Authorization. You have to include access token to the HTTP header and note that leading bearer is required. You must include your access token in HTTP header in every request you make. The token is unique, issued for each client, and regularly changed(once a year). To obtain an access token, please upgrade your plan to Professional or Premium plan. You'll be able to see your access token on the API tab of your profile page after the subscription.
HTTP Authorization Scheme: bearer
Bearer format: JWT

usage python:
import requests
headers = {'Authorization': 'Bearer ' + access_token}
url = "https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-inflow-cdd?exchange=binance&window=day&from=20191001&limit=2"
print(requests.get(url, headers=headers).json())