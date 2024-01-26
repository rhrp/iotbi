var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var fs=require('fs');
var indexRouter = require('./routes/index');
var apiRouter = require('./routes/api');
var apiConfig = require('./lib/apiConfig.js');
var app = express();
var debug = require('debug')('iotbi.app');

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

// create a write stream (in append mode)
var lAccessLogStream = fs.createWriteStream(apiConfig.logger.access.file, { flags: 'a' })

app.use(logger(apiConfig.logger.access.format,{ stream: lAccessLogStream }));	// combined format and options
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/api', apiRouter);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404,'This endpoint does not exist'));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
