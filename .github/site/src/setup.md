# Setup

To use Goral you need to have a Google account and obtain a service account:
1) Create a project https://console.cloud.google.com/projectcreate (we suggest creating a separate project for Goral for security reasons)
2) Enable Sheets from the products page https://console.cloud.google.com/workspace-api/products
3) Create a service account https://console.cloud.google.com/workspace-api/credentials with Editor role
4) After creating the service acoount create a private key (type JSON) for it (the private key should be downloaded by your browser)
5) Create a spreadsheet (where the scraped data will be stored) and add the service account email as an Editor to the spreadsheet
6) Extract spreadsheet id from the spreadsheet url

Steps 5-6 can be repeated for each [service](./services.md) (it is recommended to have separate spreadsheets for usability and separation of concerns).

Note: you can also install Google Sheets app for your phone to have an access to the data.

Notifications are sent to messengers with three levels:
* 🟢 (INFO)
* 🟡 (WARN)
* 🔴 (ERROR)

and are prefixed with id (the argument for `--id` flag).