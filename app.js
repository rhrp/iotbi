/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module defines all aspects of the app web  
 */
var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var fs=require('fs');
var indexRouter = require('./routes/index');
var apiRouter = require('./routes/api');
var webhdfsV1Router = require('./routes/webhdfs_v1');
var configsys = require('./lib/configsys.js');
var app = express();
//var compression = require('compression')
var debug = require('debug')('iotbi.app');

// compress responses
//app.use(compression())

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

// create a write stream (in append mode)
var lAccessLogStream = fs.createWriteStream(configsys.logger.access.file, { flags: 'a' })

app.use(logger(configsys.logger.access.format,{ stream: lAccessLogStream }));	// combined format and options
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/api', apiRouter);
app.use('/webhdfs/v1', webhdfsV1Router);
app.use('/', indexRouter);
// all AND !/api AND AND !webhdfs
app.use('(/*)', indexRouter);

// catch 403 and forward to error handler
app.use(function(req, res, next) {
  debug('Forbidden access to this page or endpoint: "'+req.path+'"');
  next(createError(403,'Forbidden access to this page or endpoint'));
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  debug('This page or endpoint does not exist: "'+req.path+'"');
  next(createError(404,'This page or endpoint does not exist'));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};
  debug('Will send error: '+JSON.stringify(err));
  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
