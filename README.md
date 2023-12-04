# DATA Broker Backend

The DATA Broker backend is a collection of services that power the Data Broker's central data submission platform.

The website that runs on these services is here: [https://broker.usaspending.gov/](https://broker.usaspending.gov/ "DATA Broker website").

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

One of these tools is the Data Broker. The Broker ingests federal spending data from agency award and financial systems, validates it, and standardizes it against the [common DATA Act model](http://fedspendingtransparency.github.io/data-model/ "data model"). Treasury will make a hosted version of the Broker freely available to agencies. Alternately, agencies can take this code and run the Broker locally.

The Broker contains:

* **The [Data Broker core](dataactcore/ "Data Broker core"):** common models and services used by the Broker
* **The [Broker's application programming interface (API)](dataactbroker/ "Data Broker API"):** data submission API
* **The [Data Broker validator](dataactvalidator/ "Data Broker Validator"):** data validation service
* **The [Broker website](https://github.com/fedspendingtransparency/data-act-broker-web-app "Data Broker website"):** data submission and reporting website powered by the above

The first three items compose the Broker's backend and are maintained in this repository. For details about any of the above, please follow the links to their individual README files.

## Using the DATA Broker

### Using Treasury's Hosted Broker

If you're from a federal agency that will use Treasury's hosted Data Broker, you can probably stop reading here. Instead, visit the [Broker's website](https://broker.usaspending.gov/ "Data Broker") to request a user account.

### Installing the Broker Locally

If you want to install the software on your own machine, follow the instructions on our DATA Broker [install guide](doc/INSTALL.md "INSTALL.md").
