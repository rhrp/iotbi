"use strict";

var fetch = require('node-fetch');

class OCB { 
    constructor(){
        this._host  = null;
        this._port = null;
        this._version = null;
        this._connect = null;
    }

    downloadModel(url) {
        let t = this
        const promise = new Promise(function (resolve, reject) {
            const options = {
                method: 'GET',
                headers: {
                    'Access-Control-Allow-Methods':'GET, POST, OPTIONS, PUT, PATCH, DELETE'
                },
            };
            fetch(url, options)
            .then(function(res) {              
                if(res.status >= 200 && res.status <= 208){
                    resolve(res.json());
                }else{
                    reject({status : res.status , message : res.statusText});
                }
                
            })
            .catch((err) => {
                reject(new Error(`An error has occurred in the search for the entity: ${err}`))
            });
        })
        return promise

    }
    getWithQuery(query, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE';
            const options = {
                method: 'GET',
                headers: headers,
            };
            let temHeader = {};
            fetch(t._URLContext+`/entities${query}`, options)
                .then(async function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        temHeader = res.headers;
                        return res.json();
                    }else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .then(async (res) => {
                    let response = {
                        body : res,
                        headers : temHeader
                    }
                    resolve(response)
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entity: ${err}`))
                });
        })
        return promise
    }
    queryEntitiesOnArea() {
        let polygon = arguments[0]
        let query = "?"
        if (arguments[1] !== undefined && arguments[2] === undefined){
            if (typeof arguments[1] === 'boolean'){
                if(arguments[1]){
                    query+="options=keyValues&"
                }
            }else {
                return new Error('A value boolean was expected')
                //return new Error('Se esperaba valor boleano')
            }
        }else if (arguments[1] !== undefined && arguments[2] !== undefined ) {
                if (arguments[3] !== undefined && arguments[3]) 
                    query+="options=keyValues&"
                query+= `id=${arguments[1]}`
                query+= `&type=${arguments[2]}&`
            }else if(
                (arguments[1] === undefined && arguments[2] !== undefined) || 
                (arguments[1] !== undefined && arguments[2] === undefined)
            )
                return new Error('Error in the number of parameters')
                //return new Error('Error numero de parametros')
            query+='georel=coveredBy&geometry=polygon&coords='
            query+=polygon.join(';')
        //return query;
        //console.log(query)
        let t = this 
        const promise = new Promise(function (resolve, reject) {

            const options = {
                method: 'GET',
                headers: {
                    'Access-Control-Allow-Methods':'GET, POST, OPTIONS, PUT, PATCH, DELETE'
                },
            };
            
            fetch(t._URLContext+`/entities${query}`, options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json());
                    }
                    else if (res.status === 404){
                        reject(new Error("The entities has not been found"));
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entities: ${err}`))
                });
        })
        return promise

    }  
    config(_url, headers = {}){
        this._URLContext= _url;
        return this.testConnect(headers);
    }
    testConnect(headers = {}){
        let t = this 
        headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE';
        const options = {
            method: 'GET',
            headers: headers,
        };
        const promise = new Promise(function (resolve, reject) {
            fetch(t._URLContext, options)
            .then((res) => {
                if(res.status >= 200 && res.status <= 208){
                    resolve({status : res.status, message: "Connection successful with the Orion ContextBroker"})
                }
                else if (res.status === 404){
                    reject(new Error("Request failed. URL not found"))
                }
                else if(res.status >= 500 && res.status <= 521){
                    reject(new Error("The server failed to complete the request"))
                }
                else{
                    reject({status : res.status , message : res.statusText});
                }
            })
            .catch((err)=>{
                reject(new Error(`An error has occurred in the connection: ${err}`))
            })
        })
        return promise
    }    
    retrieveAPIResources(headers = {}){
        let t = this 
        const options = {
            method: 'GET',
            headers: headers,
        };
        const promise = new Promise(function (resolve, reject) {
            fetch(t._URLContext, options)
            .then((res) => {
                if(res.status >= 200 && res.status <= 208){
                    resolve(res.json())
                }
                else if (res.status === 404){
                    reject(new Error("Request failed. Resources NOT Found"))
                }
                else if(res.status >= 500 && res.status <= 521){
                    reject(new Error("The server failed to complete the request"))
                }
                else{
                    reject({status : res.status , message : res.statusText});
                }
            })
            .catch((err)=>{
                reject(new Error(`An error has occurred in the connection: ${err}`))
            })
        })
        return promise
    }
    addTimeStampEntity(entity){
        let timeStamp = 
            {
                "value": new Date().toISOString(),
                "type" : "DateTime"
            };
        entity['dateCreated'] = timeStamp;
        return entity;
    }
    createEntity(entity, headers = {}){
        //Add timestamp attribute to all entity that will be created
        this.addTimeStampEntity(entity);
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Content-Type'] =  'application/json';
            const options = {
                method: 'POST',
                headers: headers,
                body: JSON.stringify(entity)
            };
            fetch(t._URLContext+'/entities', options)
                .then(function(res) {                  
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message:"Entity created successfully"})
                    }
                    else if (res.status === 422){
                        reject(new Error("Existing entity with this ID: "+entity.id))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred with the creation of the entity: ${err}`))
                });

        })
        return promise            
    }
    getEntity(idEntity ,headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/entities/'+idEntity, options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json());
                    }
                    else if (res.status === 404){
                        reject(new Error("The entity with 'id': "+idEntity+ " has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entity: ${err}`))
                });
        })
        return promise

    }
    getEntityAttrs(idEntity, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs', options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json());
                    }
                    else if (res.status === 404){
                        reject(new Error("The entity with 'id': "+idEntity+ " has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entity: ${err}`))
                });
        })
        return promise

    }
    getEntityAttribute(idEntity, attribute, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/'+attribute, options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json());
                    }
                    else if (res.status === 404){
                        reject(new Error("The entity with 'id': "+idEntity+ " has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entity: ${err}`))
                });
        })
        return promise
    }
    getEntityAttributeValue(idEntity, attribute, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/'+attribute+'/value', options)
                .then( function(res) {  
                    if(res.status >= 200 && res.status <= 208){
                        return res.json();  
                    }
                    else if (res.status === 404){
                        reject(new Error("The entity or the attribute specified hast not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .then( function (value){
                    resolve({value: value, attribute: attribute});
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entity: ${idEntity} Error: ${err}`))
                });
        })
        return promise
    }
    deleteEntityAttribute(idEntity, attribute, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/'+attribute, options)
                .then(function(res) {
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message: `Has been deleted the attribute: ${attribute} of the entity: ${idEntity}`})
                    }
                    else if (res.status === 404){
                        reject(new Error("The attribute  or entity has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred when removing the attribute: ${attribute} of the entity: ${idEntity} Error: ${err}`));
                });
        })
        return promise
    }
    //Get EntityTypes
    getEntityTypes(headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Accept-Type'] = 'application/json' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/types', options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.text())
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entity types: ${err}`))
                });
        })
        return promise
    }
    //Get EntityType
    getEntityType(entityType, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Accept-Type'] = 'application/json' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            console.log(t._URLContext+'/types/'+entityType)
            fetch(t._URLContext+'/types/'+entityType, options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.text())
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has occurred in the search for the entities: ${entityType} Error: ${err}`))
                });
        })
        return promise
    }
    //Get entities list of an entity type
    getEntityListType(typeEntity , headers ={}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Accept-Type'] = 'application/json' ; 
            const options = {
                method: 'GET',
                headers: headers,
            };
            fetch(t._URLContext+'/entities?type='+typeEntity, options)
                .then(function(res) {              
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json());
                    }
                    else if (res.status === 404){
                        reject(new Error("The entities of 'type': "+typeEntity+ " has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err) => {
                    reject(new Error(`An error has ocurred in the search for the entities: ${err}`))
                });
        })
        return promise

    }
    deleteEntity(idEntity, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'DELETE',
                headers: headers,
            };
            fetch(t._URLContext+'/entities/'+idEntity, options)
                .then(function(res) {
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message: "Has been deleted the entity: "+idEntity})
                    }
                    else if (res.status === 404){
                        reject(new Error("The entity with 'id': "+idEntity+ " has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred to delete the entity: ${err}`));
                });
        })
        return promise
    }
    //updateOrAppendEntityAttributes - POST /entities/{entityId}/attrs
    addJSONAttributeToEntity(idEntity, JSONAttribute, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Content-Type'] = 'application/json' ; 
            const options = {
                method: 'POST',
                headers: headers,
                body: JSON.stringify(JSONAttribute)
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs', options)
                .then(function(res) {                    
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message : "The JSON object has been added to the entity with 'id':"+idEntity})
                    }
                    else if (res.status === 404){
                        reject(new Error("The entity with 'id':"+idEntity+" has not been found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred to add a JSON attribute to the entity: ${idEntity}. Error: ${err}`))
                });

        })
        return promise
    }
    //updateAttributeData - PUT /entities/{entityId}/attrs/{attrName}	
    updateJSONAttrEntity(idEntity, nameAttribute, jsonAttr, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Content-Type'] = 'application/json' ; 
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'PUT',
                headers: headers,
                body: JSON.stringify(jsonAttr)
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/'+nameAttribute, options)
                .then(function(res) {             
                    if(res.status >= 200 && res.status <= 208){
                        resolve({statusCode : res.status , message : "The JSON object has been updated the entity with 'id':"+idEntity})
                    }
                    else if (res.status === 404){
                        reject(new Error("Request failed. Entity or attribute not found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })                
                .catch(function(err){
                    reject(new Error(`An error has occurred to get the entity updated: ${err}`));
                });
        })
        return promise
    }
    //updateExistingEntityAttributes - 	PATCH /entities/{entityId}/attrs
    updateEntityAttrs(idEntity, jsonObjectAttrs, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Content-Type'] = 'application/json' ; 
            const options = { 
                method: 'PATCH',
                headers: headers,
                body: JSON.stringify(jsonObjectAttrs)
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/', options)
                .then(function(res) {             
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message : "The attributes of entity with 'id': "+idEntity+ " have been updated successfully"})
                    }
                    else if (res.status === 404){
                       reject(new Error("Request failed. Entity not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred to update the entity attributes: ${err}`))
                });
        })
        return promise
    }
    //updateAttributeValue - PUT /entities/{entityId}/attrs/{attrName}/value
    updateEntityAttributeValue(idEntity, nameObjectAttribute, val, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            const valueString = val.toString();
            const valueLength = valueString.length;
            headers['Content-Type'] = 'text/plain' ; 
            headers['Content-Length'] = valueLength;
            const options = {
                method: 'PUT',
                headers: headers,
                body: valueString
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/'+nameObjectAttribute+'/value', options)
                .then((res) => {  
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message : "The attribute 'value' of the object: " + nameObjectAttribute + " that belongs to the entity with 'id': " + idEntity+ " has been updated successful"})
                    }
                    else if (res.status === 404){
                        reject(new Error("Request failded. Etntity or attribute not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred to update the attributte value ${nameObjectAttribute} that corresponds to the entity ${idEntity}: ${err}`));
                });

        })
        return promise
    }
    replaceAllEntityAttributes(idEntity, jsonObjectAttrs, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Content-Type'] = 'application/json' ; 
            const options = { 
                method: 'PUT',
                headers: headers,
                body: JSON.stringify(jsonObjectAttrs)
            };
            fetch(t._URLContext+'/entities/'+idEntity+'/attrs/', options)
                .then(function(res) {             
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message: "The attributes of the entity with id: "+idEntity+ " has been replaced successfully"})
                    }
                    else if (res.status === 404){
                       reject(new Error("Request failed. Entity not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred replacing the attributes of the entity: ${idEntity} Error: ${err}`))
                });
        })
        return promise
    }
    listEntities(headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Accept-Type'] = 'application/json' ; 
            const options = {
                method: 'GET',
                headers: headers
            };
            fetch(t._URLContext+'/entities/', options)
                .then(function(res) {                    
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json())
                    }
                    else if (res.status === 404){
                        reject(new Error("Request failed. Entities not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err)=>{
                    //console.log(`Ha ocurrido un error al obtener el listado de entidades: ${err}`);
                    reject(new Error(`An error has occurred to get the entities list: ${err}`));
                });
        })
        return promise
    }
    // SUBSCRIPTIONS SECTION ======
    //Create a suscription
    createSubscription(subscription, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Content-Type'] = 'application/json' ; 
            const options = {
                method: 'POST',
                headers: headers,
                body: JSON.stringify(subscription)
            };
            fetch(t._URLContext+'/subscriptions', options)
                .then(function(res) {                    
                    if(res.status >= 200 && res.status <= 208){
                        let id = res.headers._headers.location[0].split("/")[3];
                        resolve({
                            status : res.status,
                            message: "Subscription created successfully",
                            id 
                        })
                    }
                    else if (res.status === 422){
                        reject(new Error("Existing subscription ID"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred to create the subscription: ${err}`))
                });
        })
        return promise            
    }
    //Get the subscriptions list
    listSubscriptions( headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Accept-Type'] = 'application/json' ; 
            const options = {
                method: 'GET',
                headers: headers
            };
            fetch(t._URLContext+'/subscriptions/', options)
                .then(function(res) {                    
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json())
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err)=>{
                    reject(new Error(`Has occurred an error to get the subscriptions list: ${err}`));
                });
        })
        return promise
    }
    getSubscription(idSubscription, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Accept-Type'] = 'application/json' ; 
            const options = {
                method: 'GET',
                headers: headers
            };
            fetch(t._URLContext+'/subscriptions/'+idSubscription, options)
                .then(function(res) {                    
                    if(res.status >= 200 && res.status <= 208){
                        resolve(res.json())
                    }
                    else if (res.status === 404){
                        reject(new Error("Request failed. Subscription not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }
                })
                .catch((err)=>{
                    reject(new Error(`An error has occurred to get the subscription with 'id': ${idSubscription} Error: ${err}`));
                });
        })
        return promise
    }
    updateSubscriptionStatus(idSubscription, status, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Content-Type'] = 'application/json' ; 
            const options = {
                method: 'PATCH',
                headers: headers,
                body: JSON.stringify({
                    "status": status
                })
            };
            fetch(t._URLContext+'/subscriptions/'+idSubscription, options)
                .then(function(res) { 
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message: "The subscription: "+idSubscription+" has change to status: "+status})
                    }
                    else if (res.status === 404){
                        reject(new Error("Request failed. Subscription not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }  
                })
                .catch((err)=> {
                    reject(new Error(`Has occurred an error to inactivate the subscription: ${idSubscription} Error: ${err}`));
                });
        })
        return promise
    }
    updateSubscription(idSubscription, jsonSubscription, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            headers['Content-Type'] = 'application/json' ; 
            const options = { 
                method: 'PATCH',
                headers: headers,
                body: JSON.stringify(jsonSubscription)
            };
            fetch(t._URLContext+'/subscriptions/'+idSubscription, options)
                .then(function(res) {             
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message: "The subscription: "+idSubscription+" has been updated successfully"})
                    }
                    if (res.status === 404){
                       reject(new Error("Request failed. Subscription not found"))
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }  
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred to update the subscription: ${idSubscription}. Error: ${err}`))
                });
        })
        return promise
    }
    deleteSubscription(idSubscription, headers = {}){
        let t = this
        const promise = new Promise(function (resolve, reject) {
            headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, PATCH, DELETE' ; 
            const options = {
                method: 'DELETE',
                headers: headers,
            };
            fetch(t._URLContext+'/subscriptions/'+idSubscription, options)
                .then(function(res) {
                    if(res.status >= 200 && res.status <= 208){
                        resolve({status : res.status, message: "Has been removing the subscription with 'id': "+idSubscription})
                    }
                    else if (res.status === 404){
                        reject(new Error("The subscription with 'id': "+idSubscription+ " has not been found"));
                    }
                    else if(res.status >= 500 && res.status <= 521){
                        reject(new Error("The server failed to complete the request"))
                    }
                    else{
                        reject({status : res.status , message : res.statusText});
                    }  
                })
                .catch(function(err){
                    reject(new Error(`An error has occurred removing the subscription: ${err}`));
                });
        })
        return promise
    }
}
module.exports = new OCB()
// es6 default export compatibility
module.exports.default = module.exports;
