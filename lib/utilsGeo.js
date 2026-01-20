/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module provides geolocation utilities
 */
var turf7 = require("@turf/turf");
var schema = require('./schema.js');
var debug = require('debug')('iotbi.utilsGeo');

exports.toTableGetMetadata=toTableGetMetadata;
exports.calcDistance=calcDistance;
exports.calcDistanceMeters=calcDistanceMeters;
exports.calcDistanceKms=calcDistanceKms;

//TODO: make configurable
//"meters" | "metres" | "millimeters" | "millimetres" | "centimeters" | "centimetres" | "kilometers" | "kilometres" | "miles" | "nauticalmiles" | "inches" | "yards" | "feet" | "radians" | "degrees"
const UNIT_KMS       = "kilometers"
const UNIT_METERS    = "meters"
const UNITS	     = "kilometers";
const MIN_DISTANCE   = 20;	// 20kms
const MAX_CLUSTERS   = 30;


function calcDistanceKms(pPointA,pPointB)
{
   return calcDistance(pPointA,pPointB,UNIT_KMS);
}
function calcDistanceMeters(pPointA,pPointB)
{
   return calcDistance(pPointA,pPointB,UNIT_METERS);
}

function calcDistance(pPointA,pPointB,pUnit)
{
   var lOptions = { units: pUnit };
   var lDistance = turf7.distance(pPointA,pPointB,lOptions);
   return lDistance;
}
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
 *  TODO: Test the method.  At this moment is not used.
 */
function calcClustersOptimal(pPoints)
{
    var lOptions = { units: UNITS };
    var lOneCluster=calcClusters(pPoints,1);
    var lMaxDistance=calcMaxDistance(pPoints,getCentroids(lOneCluster)[0]);
    if(lMaxDistance<=MIN_DISTANCE)
    {
       debug('All '+pPoints.length+' points are at the maximum of '+lMaxDistance+' '+UNITS+'. A single cluster is required');
       return lOneCluster;
    }
    else
    {
       debug('All '+pPoints.length+' points are at the maximum of '+lMaxDistance+' '+UNITS+'. Several clusters are required');
    }
    //Try for 2 to MAX_CLUSTERS
    var lMaxDistanceBetweenClusters=undefined;
    var lClusters;
    var lCentroids;
    var c=1; 
    for(c=2;c<MAX_CLUSTERS;c++)
    {
        lMaxDistanceBetweenClusters=undefined;
        lClusters=calcClusters(pPoints,c);
        lCentroids=getCentroids(lClusters);
        debug('Try '+c+' clusters Number Of Points='+lClusters.features.length+'  Centrois='+JSON.stringify(lCentroids));
        for(i=0;i<lCentroids.length;i++)
        {
           for(j=i;j<lCentroids.length;j++)
           {
              if(i!=j)
              {
                var lDistance = turf7.distance(lCentroids[i],lCentroids[j], lOptions);
                if(lMaxDistanceBetweenClusters==undefined || lDistance>lMaxDistanceBetweenClusters)
                {
                   lMaxDistanceBetweenClusters=lDistance;
                }
              }
           }
        }
        if(lMaxDistanceBetweenClusters>MIN_DISTANCE)
        {
           debug('All '+c+' clusters are distant of '+lMaxDistanceBetweenClusters+' '+UNITS);
           return lClusters; 
        }
        else
        {
           debug('Not all '+c+' clusters are distant of '+lMaxDistanceBetweenClusters+' '+UNITS);
        }
    }
    // All locations are near, less clusters will be consired 
    return c==1?lOneCluster:lClusters;
}
/**
 * Find the clusters in a way that all point in the cluster are distant to the centroid of a maximum radius 
 */
function calcClustersInsideRadius(pPoints,pRadius)
{
    var lOptions = { units: UNITS };
    var lOneCluster=calcClusters(pPoints,1);
    var lMaxDistance=calcMaxDistance(pPoints,getCentroids(lOneCluster)[0]);
    if(lMaxDistance<=pRadius)
    {
       //debug('All '+pPoints.length+' points are at the maximum of '+lMaxDistance+' '+UNITS+'. There is a single cluster to the required radius of '+pRadius+' '+UNITS);
       return lOneCluster;
    }
    else
    {
       //debug('All '+pPoints.length+' points are at the maximum of '+lMaxDistance+' '+UNITS+'. The are several clusters to the required radius of '+pRadius+' '+UNITS);
    }
    //Try for 2 to MAX_CLUSTERS
    var lMaxDistantPoint=undefined;
    var lClusters;
    var lCentroids;
    var c=1; 
    for(c=2;c<MAX_CLUSTERS;c++)
    {
        lMaxDistantPoint=undefined;
        lClusters=calcClusters(pPoints,c);
        lCentroids=getCentroids(lClusters);
        //debug('Try '+c+' clusters Number Of Points='+lClusters.features.length+'  Centrois='+JSON.stringify(lCentroids));
        for(i=0;i<lCentroids.length;i++)
        {
           //Test cluster i
           //debug(' * Test Cluster '+i+' of '+lCentroids.length); 
           var lClusterPoints=getPointsOfCluster(lClusters,i)
           var n=0;
           for (lPoint of lClusterPoints)
           {
              // Test point j of cluster i
              var lDistance = turf7.distance(lCentroids[i],lPoint, lOptions);
              if(lMaxDistantPoint==undefined || lDistance>lMaxDistantPoint)
              {
                 lMaxDistantPoint=lDistance;
              }
              //debug('    *Test Point '+n+' of '+lClusterPoints.length+' (Centroid: '+JSON.stringify(lCentroids[i])+' Point: '+JSON.stringify(lPoint)+') Distance='+lDistance+'   lMaxDistantPoint='+lMaxDistantPoint);
              n=n+1;
           }
        }
        if(pRadius>=lMaxDistantPoint)
        {
           //debug('All '+c+' clusters have a maximum distant point ('+lMaxDistantPoint+') inside a radius of '+pRadius);
           return lClusters; 
        }
        else
        {
           //debug('Not all '+c+' clusters have a maximum distant point ('+lMaxDistantPoint+') inside a radius of '+pRadius);
        }
    }
    // All locations are near, less clusters will be consired 
    return c==1?lOneCluster:lClusters;
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
function toTableGetMetadata(pFiwareService,pEntityType,pRadius,pPoints)
{
     debug('toTableGetMetadata('+pFiwareService+','+pEntityType+','+pRadius+',[points])');    
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
       var lClusters;
       if(pRadius==undefined)
       {
          debug('Find the best clusters for a min distance between them of '+MIN_DISTANCE+' '+UNITS);
          lClusters=calcClustersOptimal(pPoints);
       }
       else
       {
          debug('Find the best clusters ensuring that all point are distant to the centroid of the maximum of  '+pRadius+' '+UNITS);
          lClusters=calcClustersInsideRadius(pPoints,pRadius);
       }
       var lCentroids=getCentroids(lClusters);
       for(clusterId=0;clusterId<lCentroids.length;clusterId++ )
       {
          var lCentroid = turf7.point(lCentroids[clusterId]);
          var lClusterPoints=getPointsOfCluster(lClusters,clusterId)
          //debug('Cluster #'+c+' Centroid='+JSON.stringify(lCentroids[c]));
          if(lClusterPoints.length>0)
          {
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
          else
          {
            debug('Empty cluster ignored: '+pFiwareService+' :: '+pEntityType+' :: Cluster '+clusterId);
          }
       }
     }
     var TableModel = require('./model/tablemodel.js');
     var lTableName='GeoMetadata-'+pFiwareService+'-'+pEntityType;
     debug(lTableName+'   rows='+lTable.length+' Schema='+JSON.stringify(lSchema));
     var lTable=new TableModel(lTableName,lTable,lSchema);
     return lTable;
}
