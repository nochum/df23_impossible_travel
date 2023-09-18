# df23_impossible_travel
## Sample code from Dreamforce 23 - [Actionable Insights: The Power of Real-Time Events](https://reg.salesforce.com/flow/plus/df23/sessioncatalog/page/catalog/session/1690384804706001a7az)

## Overview
This Python project highlights how to use the Salesforce LoginEventStream Pub/Sub API event to identify scenarios where attackers may access a Salesforce org using stolen credentials. It operates under the assumption that the attacker is typically not in the same locale as the victim. It evaluates login location and time for successive logins from the same user. In this demo if successive logins would have required travel at a velocity greater than 500 miles per hour, we consider this a suspicious login. Of course the use of a VPN would be a reasonable non-malicious scenario, however this surfaces these scenarios for further investigation.

The main program is impossible_travel.py. In order to run this, you will need to add valid OAuth and credential information to the credentials.py module.
