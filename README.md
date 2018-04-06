# DATA Act Broker Backend

The DATA Act broker backend is a collection of services that power the DATA Act's central data submission platform.

The website that runs on these services is here: [https://broker.usaspending.gov/](https://broker.usaspending.gov/ "DATA Act Broker website").

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

One of these tools is the DATA Act Broker (broker). The broker ingests federal spending data from agency award and financial systems, validates it, and standardizes it against the [common DATA Act model](http://fedspendingtransparency.github.io/data-model/ "data model"). Treasury will make a hosted version of the broker freely available to agencies. Alternately, agencies can take this code and run the broker locally.

The broker contains:

* **The [DATA Act core](dataactcore/ "DATA Act core"):** common models and services used by the broker
* **The [broker's application programming interface (API)](dataactbroker/ "DATA Act broker API"):** data submission API
* **The [DATA Act validator](dataactvalidator/ "DATA Act validator"):** data validation service
* **The [broker website](https://github.com/fedspendingtransparency/data-act-broker-web-app "DATA Act Broker website"):** data submission and reporting website powered by the above

The first three items compose the broker's backend and are maintained in this repository. For details about any of the above, please follow the links to their individual README files.

## Using the DATA Act Broker

### Using Treasury's Hosted Broker

If you're from a federal agency that will use Treasury's hosted DATA Act broker, you can probably stop reading here. Instead, visit the [broker's website](https://broker.usaspending.gov/ "DATA Act Broker") to request a user account.

### Using a Broker Virtual Image (Coming Soon)

If you want to run the broker locally, the easiest way to get started is to use the self-contained package that includes all code, servers, and dependencies in an isolated virtual image.
**Coming soon.**

### Installing the Broker Locally

If you want to install the software on your own machine, follow the instructions on our DATA Broker [contributing guide](doc/CONTRIBUTING.md#data-act-broker-setup-for-developers "DATA Act contributing guide"). If you're a developer on the DATA Act team or if you want to contribute to the project, this is the option you want.

**[Local installation directions](doc/CONTRIBUTING.md#data-act-broker-setup-for-developers "DATA Act contributing guide")**
