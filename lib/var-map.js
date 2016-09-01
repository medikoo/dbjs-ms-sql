'use strict';

var sql = require('mssql');

exports.string = sql.NText;
exports.dateTime = sql.DateTime2;
exports.regExp = sql.NText;
exports.function = sql.NText;
exports.object = sql.NVarChar(4000);
exports.multiple = sql.NText;
