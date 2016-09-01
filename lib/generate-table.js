'use strict';

var endsWith = require('es5-ext/string/#/ends-with')
  , sql      = require('mssql');

module.exports = function (name) {
	var table = new sql.Table(name);
	table.columns.add('OwnerId', sql.VarChar(8000), { nullable: false });
	table.columns.add('Path', sql.NVarChar(4000), { nullable: true });
	table.columns.add('Stamp', sql.BigInt, { nullable: false });
	table.columns.add('ValueBoolean', sql.TinyInt, { nullable: true });
	table.columns.add('ValueNumber', sql.Float, { nullable: true });
	table.columns.add('ValueString', sql.NText, { nullable: true });
	table.columns.add('ValueDateTime', sql.DateTime2, { nullable: true });
	table.columns.add('ValueRegExp', sql.NText, { nullable: true });
	table.columns.add('ValueFunction', sql.NText, { nullable: true });
	table.columns.add('ValueObject', sql.NVarChar(4000), { nullable: true });
	if (endsWith.call(name, '_Computed')) {
		table.columns.add('ValueMultiple', sql.NText, { nullable: true });
	}
	table.columns.add('ValueType', sql.TinyInt, { nullable: true });
	return table;
};
