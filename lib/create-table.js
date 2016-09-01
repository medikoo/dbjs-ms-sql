'use strict';

var endsWith      = require('es5-ext/string/#/ends-with')
  , sql           = require('mssql')
  , generateTable = require('./generate-table');

module.exports = function (connection, name) {
	var request = new sql.Request(connection)
	  , table = generateTable(name);
	var query = "if object_id('" + table.path.replace(/'/g, '\'\'') +
		"') is null " + table.declare().slice(0, -1);
	if (endsWith.call(name, '_Computed')) {
		query += ', CONSTRAINT IX_' + name + '_Id UNIQUE CLUSTERED(Path, OwnerId)';
	} else {
		query += ', CONSTRAINT IX_' + name + '_Id UNIQUE CLUSTERED(OwnerId, Path)';
	}
	query += ')';
	return request.queryPromised(query);
};
