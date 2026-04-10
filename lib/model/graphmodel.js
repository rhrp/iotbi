/**
 * API for providing Matadata
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.graphmodel');
var z = require('zod/v4');

module.exports = class Graph {

   static NGSI_TYPE     = 'NGSI_Type';		// NGSI Type
   static NGSI_ENTITY   = 'NGSI_Entity';	// Class of objects the represent NGSI entities
   static NGSI_PROPERTY = 'NGSI_Property';	// Class of objects the represent propoerties of NGSI entities

  /**
   *
   */
   constructor(name) 
   {
      this.name = name;
      this.nodes={};
      this.edges=[];
   }
   getSchemaZ()
   {
      var lNodes={};
      for(var id in this.nodes)
      {
         var lNode=this.getNodeById(id);
         lNodes[id]=Node.getSchemaZ().optional();
      }
      var sGraph=z.object({ 
             name: z.string(),
             nodes: z.object(lNodes),
             edges: z.array(Edge.getSchemaZ())
           });
      return sGraph;
   }
   toZ()
   {
      return this.getSchemaZ().parse({'name': this.name, 'nodes': this.nodes,'edges':this.edges}); 
   }
  /**
   *
   */
   print() 
   {
     console.log('GraphName: '+this.name);
     for(var id in this.nodes)
     {
        var lNode=this.getNodeById(id)
        console.log(lNode.id+'\t'+lNode.type);
        for(var prop in lNode.properties)
        {
           console.log('\t'+prop+' '+lNode.properties[prop]);
        }
        for(var lEdge of this.getEdgesByNodeId(id))
        {
           console.log('\t\tRel: '+lEdge.toString());
        }
     }
   }
  /**
   * Returns the neighborhood subgraph of node_id
   */
   neighborhood(node_id)
   {
       var lSubgraph=new Graph('neighborhood of '+node_id);
       lSubgraph.addNodeObject(node_id,this.getNodeById(node_id));
      
       for(var lEdge of this.getEdgesByNodeId(node_id))
       {
         if(lEdge.src_id==node_id)
         {
             var lNodeDst=this.getNodeById(lEdge.dst_id);
             lSubgraph.addNodeObject(lEdge.dst_id,lNodeDst);
             lSubgraph.addEdge(lEdge);
         }
         else if(lEdge.dst_id==node_id)
         {
             var lNodeSrc=this.getNodeById(lEdge.src_id);
             lSubgraph.addNodeObject(lEdge.src_id,lNodeSrc);
             lSubgraph.addEdge(lEdge);
         }
         else
         {
             debug('Warning: This is not a valid Edge of '+node_id);
         }
       }
       return lSubgraph;
   }

  /**
   * Get a node by its Id
   */
   getNodeById(id)
   {
      return this.nodes[id];
   }
  /**
   *
   */
   getEdgesByNodeId(id)
   {
      var lEdges=[];
      for(var lEdge of this.edges)
      {
          if(lEdge.src_id==id || lEdge.dst_id==id)
          {
             lEdges.push(lEdge);
          }
      }
      return lEdges;
   }
  /**
   * Add a Node
   */
   addNode(id,type)
   {
      var lNode = new Node(id,type);
      this.nodes[id]=lNode;
      return lNode;
   }
   addNodeObject(id,pNode)
   {
      this.nodes[id]=pNode;
      return pNode;
   }
   addRelationship(src_id,dst_id,relation_type)
   {
      var lNodeSrc=this.getNodeById(src_id);
      var lNodeDst=this.getNodeById(dst_id);
      if(lNodeSrc==undefined || lNodeDst==undefined)
      {
         debug('Invalid Edge '+src_id+'/'+dst_id);
         return undefined;
      }
      var lEdge = new Edge(src_id,dst_id,relation_type);
      this.addEdge(lEdge);
      return lEdge;
   }
   addEdge(pEdge)
   {
     this.edges.push(pEdge);
   }
}

/**
 * Node
 */
class Node {
  constructor(id,type) {
    this.id = id;
    this.type = type;
    this.properties={};
  }
  addProperty(key,value)
  {
    this.properties[key]=value;
  }
  static getSchemaZ()
  {
     var sProperties=z.object({
          name:z.string().optional(),
          urlSchema:z.url().optional(),
          title:z.string().optional(),
          description:z.string()
       });
     return z.object({ 
             id: z.string(),
             type: z.string(),
             properties: sProperties
           });
  }
  toZ()
  {
    var sNode=this.getSchemaZ();
    return sNode.parse({'id': this.id, 'type': this.type,'properties':this.properties }); 
  }
}
/**
 * Edge
 */
class Edge {
  constructor(src_id,dst_id,type) {
    this.src_id = src_id;
    this.dst_id = dst_id;
    this.type = type;
    //debug(this.src_id + ' ['+this.type+'] '+this.dst_id);
  }
  toString()
  {
    return this.src_id + ' ['+this.type+'] '+this.dst_id;
  }
  static getSchemaZ()
  {
     return z.object({ 
             src_id: z.string(),
             dst_id: z.string(),
             type: z.string()
           });
  }
  toZ()
  {
    var sEdge=Edge.getSchemaZ();
    return sEdge.parse({'src_id':this.src_id,'dst_id': this.dst_id,'type': this.type}); 
  }
}

