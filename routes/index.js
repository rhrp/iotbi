/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the web service index route. 
 */

var express = require('express');
var router = express.Router();
const debug = require('debug')('iotbi.route.www');

/* GET home page. */
router.get('/', function(req, res, next) {
  debug('Serving index page');
  res.render('index', { title: 'API IoT BI' });
});
router.get('/*.(html|htm|gif|jpg|png)', function(req, res, next) {
  debug('Serving known resources');
  res.render('error404', { title: 'Page not found',message:'Page not found',detail:'filename' });
});
router.get('/*.[a-zA-Z0-1]{0,5}', function(req, res, next) {
  debug('Serving unknown resource type');
  res.render('error404', { title: 'Page not found',message:'Page not found',detail:'filename' });
});
router.get('/*', function(req, res, next) {
  debug('Serving index page of a directory');
  res.render('error404', { title: 'Index page not found',message:'Index Page not found',detail:'filename' });
});




module.exports = router;
