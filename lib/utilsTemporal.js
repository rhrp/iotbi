/*!
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 */

/**
 * Formata o array de datas em time ms
 */
exports.formatDate = function(p)
{
   var out=[];
   for(var i=0;i<p.length;i++)
   {
         d=new Date(p[i]);
         //console.log(item_i,d.getTime());
         out.push(d.getTime());
   };        
   return out;
}
exports.formatDateVal = function(p)
{
    var d=new Date(p);
    var i=d.getTime();
    //console.log(p+'  => '+d+'  => '+i);
    return i;
}



/**
 * Calcula a data do dia anterior
 */
exports.getPreviousDay = function(date = new Date()) {
  const previous = new Date(date.getTime());
  previous.setDate(date.getDate() - 1);

  return previous;
}

/**
 *
 */
exports.parseFromDate = function(pParamFromDate)
{
   var lDate;
   if(pParamFromDate!=null && pParamFromDate=='today')
   {
	var lNow=new Date();
	// Forget the hour/min
	lDate=new Date(lNow.getFullYear(),lNow.getMonth(),lNow.getDate());
        //console.log('From Date Today='+lDate+'    now '+lNow);
   }
   else if(pParamFromDate!=null && pParamFromDate=='ever')
   {
	lDate=new Date(1970,1,1); 
   }
   else if(pParamFromDate!=null)
   {
	return pParamFromDate;
   }	
   else
   {
	//Default
	//Last 24h
	lDate=getPreviousDay();
   }
   return lDate.toISOString();
}
/**
 *
 */
exports.parseToDate = function(pParamToDate)
{
   var lDate;
   if(pParamToDate==null || pParamToDate=='today' || pParamToDate=='ever')
   {
        lDate=new Date();
   }
   else if(pParamToDate=='yesterday')
   {
        // up to 24 hours before
        lDate=getPreviousDay();
   }
   else
   {
	return pParamToDate;
   }
   return lDate.toISOString();
}

/**
 * Get the attribute of an entity ignoring case
 */ 
exports.getAttrIgnoreCase = function(pEntity,pAttrKey)
{
   for(var lEntityAttrKey in pEntity)
   {
	if(lEntityAttrKey.toLowerCase()==pAttrKey.toLowerCase())
	{
	    return pEntity[lEntityAttrKey];
	}
   }
   return undefined;
}
exports.timestampToDate = function(pTimestamp)
{
   var lDate=new Date(pTimestamp);
   //console.log('Timestamp:'+pTimestamp+'   =>  '+lDate);
   return lDate.toISOString();
}
