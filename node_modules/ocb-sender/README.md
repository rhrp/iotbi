# OCB - sender  

[![https://nodei.co/npm/ocb-sender.png?downloads=true&downloadRank=true&stars=true](https://nodei.co/npm/ocb-sender.png?downloads=true&downloadRank=true&stars=true)](https://www.npmjs.com/package/ocb-sender)

## What is ocb-sender?

ocb - sender is a npm module that handle a NGSI Object for them transportation to FIWARE Orion Context Broker. It makes possible send context information in easy way to the FIWARE Ecosystem.
***
## Indéx navigation

* [How to Install](#how-to-install)
* [Import npm module](#import-npm-module)
* [Module Usage](#module-usage)
	* [General Functions](#general-functions)
		* [Connection configuration with Orion ContextBroker](#connection-configuration-with-orion-contextbroker)
		* [Retrieve Orion ContextBroker API Rescources](#retrieve-orion-contextbroker-api-resources)
		* [Get EntityType of ContextBroker](#get-entitytype-of-contextbroker)
		* [Get EntitytTypes of ContextBroker](#get-entitytypes-of-contextbroker)
	* [Specific Functions](#specific-functions)
		* [Entities Functions](docs/EntitiesFunctions.md)
    	* [Subscriptions Functions](docs/SubscriptionsFunctions.md)
    	* [Query Functions](docs/QueryFunctions.md)
* [License](#license)

## How to install

```
npm install ocb-sender
```

## Import npm module.

ES5

```js
    var cb = require('ocb-sender');
```
ES6
```js
    import OCB as cb from  ocb-sender;
```
## Module Usage

### Headers 

For the examples we will use the next JSON as headers
```js
var headers = {
    'Fiware-Correlator': '3451e5c2-226d-11e6-aaf0-d48564c29d20'
}
```
But you can use another options,one empty JSON or you can ignore the headers if you don't need them

### Connection configuration with Orion ContextBroker.

```js
 cb.config(urlContextBroker, headers)
 .then((result) => console.log(result))
 .catch((err) => console.log(err))
 ```
> Example
```js
cb.config('http://207.249.127.149:1026/v2/', headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))
```
### Retrieve Orion ContextBroker API Rescources.
> Example
```js
cb.retrieveAPIResources(headers)
.then((result) => console.log(result))
.catch((err) console.log(err))
```
### Get EntityType of ContextBroker.
> Example
```js
cb.getEntityType("Device",  headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))
```
### Get EntityTypes of ContextBroker.
> Example
```js
cb.getEntityTypes(headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))
```

## License

MIT 



