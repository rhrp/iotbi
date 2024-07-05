/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the WebHDFS routes.
 * 
 */
const express = require('express');
const router = express.Router();
const apiWebHDFS = require('../lib/apiWebHDFS.js');
const debug = require('debug')('iotbi.route.webhdfs');


router.get('/user/:hadoopUser',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceCommandRoot]);
router.get('/:fiwareService/:entityType/joins/:attribFKey/:joinEntityType/current.:outputFormat',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.serviceFileStats,apiWebHDFS.serviceCommandCurrentDataset]);
router.get('/:fiwareService/:entityType/joins/:attribFKey/:joinEntityType',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.servicePreEntity,apiWebHDFS.serviceCommandEntityJoinAttrib]);
router.get('/:fiwareService/:entityType/joins/:attribFKey',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.servicePreEntity,apiWebHDFS.serviceCommandEntityJoinAttrib]);
router.get('/:fiwareService/:entityType/joins',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.servicePreEntity,apiWebHDFS.serviceCommandEntityJoins]);
router.get('/:fiwareService/:entityType/current.:outputFormat',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.serviceFileStats,apiWebHDFS.serviceCommandCurrentDataset]);
router.get('/:fiwareService/:entityType',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.serviceCommandEntity]);
router.get('/:fiwareService',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceWebhdfsAccess,apiWebHDFS.serviceCommandFiwareService]);
router.get('/',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceCommandRoot]);
router.get('*',[apiWebHDFS.serviceDebug,apiWebHDFS.serviceNotFound]);
module.exports = router;
