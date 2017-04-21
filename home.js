var express = require('express'),
router = express.Router(),
Article = require('../models/article'),
impala = require('node-impala'),
g = require('generic-functions');

var bodyParser = require('body-parser')
router.use( bodyParser.json() );
router.use(bodyParser.urlencoded({
  extended: true
}));

var conf;

module.exports = function (app) {
  app.use('/', router);
};
const client = impala.createClient();
client.connect({
  host: 'cdh578dn1.telkom.co.za',
  port: 21000,
  resultType: 'json-array'
});

router.get('/', function (req, res, next) {
  var articles = [new Article(), new Article()];
  res.render('index');
});

router.get('/dm', function (req, res, next) {
  var articles = [new Article(), new Article()];
  res.render('dm');
});

router.get('/dm/dbl', function (req, res) {
  getClusterDBList(res);
});

function getClusterDBList(res) {
  client.query('SHOW DATABASES')
    .then(console.log('query'))
    .done(function (results) {
      res.json(results);
    });
}

router.post('/dm/tbl', function (req, res) {
  conf = req.body;
});

router.get('/dm/tbl', function (req, res) {
  getClusterTBList(res);
});

function getClusterTBList(res) {
  client.query('SHOW TABLES IN  '+ conf.db)
    .then(console.log('query'))
    .done(function (results) {
      res.json(results);
    });
}

router.get('/dm/runtbl', function (req, res) {
  getRunTBList(res);
});

function getRunTBList(res) {
  client.query('SHOW TABLES IN  default')
    .then(console.log('query'))
    .done(function (results) {
      res.json(results);
    });
}

router.post('/dm/runcl', function (req, res) {
  conf = req.body;
});

router.get('/dm/runcl', function (req, res) {
  getRunCol(res);
});

function getRunCol(res) {
  console.log(conf.tb);
  client.query('SELECT * FROM ' + conf.tb + ' where graph is not null')
    .then(console.log('query'))
    .done(function (results) {
      res.json(results);
    });
}
router.post('/dm/img', function (req, res) {
  conf = req.body;
});

router.get('/dm/img', function (req, res) {
  getImage(res);
});

function getImage(res) {
    client.query('SELECT ' + conf.con + ' FROM default.' + conf.tb + ' WHERE colm = \'' + conf.cat + '\'')
    .then(console.log('query'))
    .done(function (results) {
      res.json(results);
      console.log(results);
    });
}
router.post('/dm/graphdata', function (req, res) {
  data = req.body;
});

router.get('/dm/graphdata', function (req, res) {
  getGraphData(res);
});

function getGraphData(res) {
    client.query('SELECT colm, col_type, uniques, missing, mean, stddev FROM default.' + data.tb + ' WHERE colm = \'' + data.cat + '\'')
    .then(console.log('query'))
    .done(function (results) {
      res.json(results);
      console.log(results);
    });
}
