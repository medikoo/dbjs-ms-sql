'use strict';

var sql = require('mssql');

module.exports = function (t, a) {
	a(t.dateTime, sql.DateTime2);
};
