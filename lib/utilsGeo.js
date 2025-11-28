/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module provides geoloaction utilities
 */
var turf7 = require("@turf/turf");
var schema = require('./schema.js');
var debug = require('debug')('iotbi.utilsGeo');

exports.toTableGetMetadata=toTableGetMetadata;

//TODO: make configurable
const UNITS	     = "kilometers";
const MIN_DISTANCE   = 50;	// 50kms

/**
 * Max distance of points to the center (or centroid) 
 */
function calcMaxDistance(pPoints,pCenter)
{
   var lMaxDistance=undefined;
   var lOptions = { units: UNITS }; 
   for (lPoint of pPoints)
   {
      //debug(JSON.stringify(lPoint));
      var lDistance = turf7.distance(pCenter,lPoint, lOptions);
      if (lMaxDistance==undefined || lDistance>lMaxDistance)
      {
         lMaxDistance=lDistance
      }
      //debug('Center '+JSON.stringify(pCenter.geometry.coordinates)+' to Point '+JSON.stringify(lPoint)+'   Distance: '+lDistance+'   MaxDistance: '+lMaxDistance);
   }
   return lMaxDistance;
}
/**
 * Min distance of points to the center (or centroid) 
 */
function calcMinDistance(pPoints,pCenter)
{
   var lMinDistance=undefined;
   var lOptions = { units: UNITS };
   for (lPoint of pPoints)
   {
      //debug(JSON.stringify(lPoint));
      var lDistance = turf7.distance(pCenter,lPoint, lOptions);
      if (lMinDistance==undefined || lDistance<lMinDistance)
      {
         lMinDistance=lDistance
      }
      //debug('Center '+JSON.stringify(pCenter.geometry.coordinates)+' to Point '+JSON.stringify(lPoint)+'   Distance: '+lDistance+'   MinDistance: '+lMinDistance);
   }
   return lMinDistance;
}

/**
 * Get n cluster of points (array of lat/long) 
 */
function calcClusters(pPoints,pNumOfClusters)
{
   var lPoints=turf7.points(pPoints);
   var lOptions = { numberOfClusters: pNumOfClusters };
   var lClustered = turf7.clustersKmeans(lPoints,lOptions);
   //debug('Clusters: '+JSON.stringify(lClustered,null,2));
   return lClustered;
}

/**
 * Get an array of centroids indexed by cluster's id 
 */
function getCentroids(pClusters)
{
   var lArray=[];
   for(lCluster of pClusters.features)
   {
      lArray[lCluster.properties.cluster]=lCluster.properties.centroid;
   }
   return lArray;
}
/**
 * Get the points of a cluster as an array of lat/long 
 */
function getPointsOfCluster(pClusters,pClusterId)
{
   var lArray=[];
   for(lCluster of pClusters.features)
   {
      if(lCluster.properties.cluster==pClusterId)
      {
         lArray.push(lCluster.geometry.coordinates);
      }
   }
   return lArray;
}

/**
 *  Find the clusters in a way that they have a minimal distance (centroid to centroid)
 */
function calcClustersOptimal(pPoints,pMinDistance)
{
    var lOptions = { units: UNITS };
    var lOneCluster=calcClusters(pPoints,1);
    var lMaxDistance=calcMaxDistance(pPoints,getCentroids(lOneCluster)[0]);
    if(lMaxDistance<=MIN_DISTANCE)
    {
       //debug('In case of one cluster the max distance of all '+pPoints.length+' points is '+lMaxDistance+' '+UNITS);
       return lOneCluster;
    }
    for(c=2;c<5;c++)
    {
        lMinDistance=undefined;
        var lClusters=calcClusters(pPoints,c);
        var lCentroids=getCentroids(lClusters);
        //debug('Try '+c+' clusters Number Of Points='+lClusters.features.length+'  Centrois='+JSON.stringify(lCentroids));
        for(i=0;i<lCentroids.length;i++)
        {
           for(j=i;j<lCentroids.length;j++)
           {
              if(i!=j)
              {
                var lDistance = turf7.distance(lCentroids[i],lCentroids[j], lOptions);
                if(lMinDistance==undefined || lDistance<lMinDistance)
                {
                   lMinDistance=lDistance;
                }
                //debug('['+c+'('+i+'/'+j+')] ## Centroid: '+JSON.stringify(lCentroids[i])+'  Centroid: '+JSON.stringify(lCentroids[j])+'   Distance='+lDistance+'   MinDistance='+lMinDistance);
              }
           }
        }
        if(pMinDistance>lMinDistance)
        {
           //debug('When '+c+' clusters they have a mininal distance of '+pMinDistance);
           return lClusters; 
        }
    }
    // All locations are near, one cluster will be consired 
    return lOneCluster;
}

function checkListOfPoints(pPoints)
{
    debug('checkListOfPoints')
    if(pPoints==undefined || pPoints.length==0)
    {
      debug('checkListOfPoints invalid list of points!')
       return false;
    }
    for (lPoint of pPoints)
    {
       if (lPoint[0]==undefined || lPoint[1]==undefined)
       {
           debug('Point '+JSON.stringify(lPoint)+' is invalid!')
           return false;
       }
       //debug('Point '+JSON.stringify(lPoint)+' is ok')
    }
    return true;
}
/**
 *
 */
function toTableGetMetadata(pFiwareService,pEntityType,pPoints)
{
     //debug('Points: \n'+JSON.stringify(pPoints,null,2));
     var lTable=[];
     var lSchema={};

     lSchema['fiwareService']=schema.STRING;
     lSchema['entityType']=schema.STRING;
     lSchema['centroid_location_coordinates_lon']=schema.DOUBLE;
     lSchema['centroid_location_coordinates_lat']=schema.DOUBLE;
     lSchema['max_distance']=schema.DOUBLE;
     lSchema['points']=schema.INTEGER;
     lSchema['cluster']=schema.INTEGER;


     if(checkListOfPoints(pPoints))
     {
       var lClusters=calcClustersOptimal(pPoints,MIN_DISTANCE);
       var lCentroids=getCentroids(lClusters);
       for(clusterId=0;clusterId<lCentroids.length;clusterId++ )
       {
          var lCentroid = turf7.point(lCentroids[clusterId]);
          var lClusterPoints=getPointsOfCluster(lClusters,clusterId)
          //debug('Cluster #'+c+' Centroid='+JSON.stringify(lCentroids[c]));
          var lRow={};
          lRow['fiwareService']=pFiwareService;
          lRow['entityType']=pEntityType;
          lRow['cluster']=clusterId;

          //debug('Center of '+pFiwareService+':\n'+JSON.stringify(lCenter,null,2));
          lRow['centroid_location_coordinates_lon']=lCentroid.geometry.coordinates[0];
          lRow['centroid_location_coordinates_lat']=lCentroid.geometry.coordinates[1];
          var lMaxDistance=calcMaxDistance(lClusterPoints,lCentroid);
          lRow['points']=lClusterPoints.length;
          lRow['max_distance']=lMaxDistance;
          lTable.push(lRow);
       }
     }
     else
     {
       var lRow={};
       lRow['fiwareService']=pFiwareService;
       lRow['entityType']=pEntityType;
       lRow['cluster']=0;
       lRow['centroid_location_coordinates_lon']=null;
       lRow['centeoid_location_coordinates_lat']=null;
       lRow['points']=0;
       lRow['max_distance']=null;
       lTable.push(lRow);
     }

     var lTableAndSchema=[lTable,lSchema];
     //debug('GeoMetadata: '+JSON.stringify(lTableAndSchema,null,2));
     return lTableAndSchema;
}
