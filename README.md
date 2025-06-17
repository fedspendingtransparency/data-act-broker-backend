# DATA Broker Backend

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![Pull Request Checks](https://github.com/fedspendingtransparency/data-act-broker-backend/actions/workflows/pull-request-checks.yaml/badge.svg)](https://github.com/fedspendingtransparency/data-act-broker-backend/actions/workflows/pull-request-checks.yaml) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/data-act-broker-backend/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/data-act-broker-backend/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/data-act-broker-backend/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/data-act-broker-backend)

The DATA Broker backend is a collection of services that power the Data Broker's central data submission platform.

The website that runs on these services is here: [https://broker.usaspending.gov/](https://broker.usaspending.gov/ "DATA Broker website").

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

One of these tools is the Data Broker. The Broker ingests federal spending data from agency award and financial systems, validates it, and standardizes it against the [Governmentwide Spending Data Model](http://fedspendingtransparency.github.io/data-model/ "data model"). Treasury will make a hosted version of the Broker freely available to agencies. Alternately, agencies can take this code and run the Broker locally.

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
