var cb  = require("./lib/OCB");

var headers = {
    'Fiware-Correlator': '3451e5c2-226d-11e6-aaf0-d48564c29d20'
}

var urlContextBroker = "http://35.185.120.11:1026/v2";  // Está mal no necesita ultima /
cb.config(urlContextBroker, headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))

/*
cb.retrieveAPIResources(headers)
.then((result) => console.log(result))
.catch((err)  => console.log(err))  // Esta mal domcumentación no pone =>

cb.getEntityType("Device")
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.getEntityTypes(headers)
.then((result) => console.dir(result))
.catch((err) => console.log(err))
*/

/*
cb.getEntityAttributeValue("UserContext:User_33", "location", headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.getEntityAttribute("UserContext:User_33", "location", headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.getEntityAttrs("UserContext:User_33", headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.getEntity("UserContext:User_33", headers)
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.getEntityListType("UserContext", headers)
.then((entities) => {console.log(entities)})
.catch((err) => console.log(err))

cb.listEntities(headers)
.then((entities) => {console.log(entities)})
.catch((err) => console.log(err))
*/


/*cb.createEntity({
    "id": "Room6",
    "temperature": {
        "metadata": {
            "accuracy": {
            "type": "Number",
            "value": 0.8
            }
        },
    "type": "Number",
    "value": 26.5
    },
    "type": "Room"
}).then((result) => console.log(result))
.catch((err) => console.log(err))
*/
/*
cb.updateEntityAttributeValue('Room1', 'temperature', 16)
.then((result) => {console.log(result)})
.catch((err) => console.log(err))

cb.updateJSONAttrEntity('Room1', 'temperature', {
    "type": "Number",
    "value": 34.982398
})
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.addJSONAttributeToEntity("Room1",{
    "pressure":{
              "value": 90,
              "type": "Integer"
        }
})
.then((result) => console.log(result))
.catch((err) => console.log(err))
*/
/*
cb.deleteEntity("Room1")
.then((result) => console.log(result))
.catch((err) => console.log(err))

cb.deleteEntityAttribute("Room1", "pressure")
.then((result) => console.log(result))
.catch((err) => console.log(err))
*/


//let query = "?id=.*&type=Device&georel=coveredBy&q=owner==Idowner&geometry=polygon&coords=18.879751306118546,-99.22197723761204;18.87991373199594,-99.22199869528413;18.87996449005033,-99.22190750017762;18.879984793267777,-99.2218270339072;18.879939111025056,-99.22174656763676;18.879893428769883,-99.22165537253022;18.87973100287282,-99.22145152464509;18.8795888800837,-99.22130132094026;18.879390923140832,-99.221076015383;18.87928940666914,-99.22097945585847;18.87893917436966,-99.22117793932557;18.87856356210443,-99.22121012583375;18.878675230703656,-99.22134960070255;18.878776747547473,-99.22145152464509;18.87888841600463,-99.22154808416965;18.87903053938793,-99.22144079580903;18.879203117619838,-99.22140860930085;18.87936554402868,-99.22153199091554;18.87948228791276,-99.22165537253022;18.879614259162025,-99.22181630507114;18.879751306118546,-99.22197723761204&options=keyValues";
//let query = "?id=Device_Smartphone_.*&type=Device&options=count";
/*let query = "?id=^Room[2-5]*&options=keyValues";
cb.getWithQuery(query, headers)
.then((result) => {
    if (result.body.length < 1){
        console.log("No entities found")
    }
    else{
        console.log(result.body)
        console.log(result.headers._headers);
    }
})
.catch((err) => console.log(err))
*/

cb.createSubscription({
    "description": "Alert subscription to QuantumLeap",
    "expires": "2040-01-01T14:00:00.00Z",
    "subject": {
      "entities": [
        {
          "idPattern": ".*",
          "type": "Alert"
        }
      ],
      "condition": {
        "attrs": []
      }
    },
    "notification": {
      "attrs": [],
      "attrsFormat": "normalized",
      "http": {
        "url": "http://192.168.191.134:8668/v2/notify"
      },
      "metadata": [
        "dateCreated",
        "dateModified"
      ]
    }
  })
  .then(console.log)
