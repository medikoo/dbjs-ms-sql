'use strict';

var forEach  = require('es5-ext/object/for-each')
  , deferred = require('deferred')
  , sql      = require('mssql')
  , varMap   = require('./var-map');

module.exports = deferred.gate(function self(connection, tableName, def, records) {
	if (!records.length) return def.resolve();

	var transaction = new sql.Transaction(connection);
	transaction.begin(function (err) {
		var count = records.length, isAborted = false;
		if (err) {
			def.reject(err);
			return;
		}
		transaction.on('rollback', function () {
			if (isAborted) return;
			def.reject(new Error("Transaction rolled back"));
		});
		var onSuccess = function () {
			transaction.commit(function (err) {
				if (err) def.reject(err);
				else def.resolve();
			});
		};
		var onFail = function (queryError) {
			if (isAborted) return;
			isAborted = true;
			transaction.rollback(function (err) {
				if (err) def.reject(err);
				else def.reject(queryError);
			});
		};
		records.forEach(function (record) {
			var request = new sql.Request(connection), query;
			if (record.path) request.input('path', sql.NVarChar(4000), record.path);
			forEach(record.params, function (value, name) {
				request.input(name, varMap[name], value);
			});
			query = 'UPDATE ' + tableName + ' SET ' +
				record.tokens.map(function (pair) { return pair[0] + ' = ' + pair[1]; }).join(', ') +
				' WHERE (OwnerId = \'' + record.ownerId + '\' AND Path ' +
				(record.path ? '= @path' : 'IS NULL') + ')';
			request.query(query, function (err, recordSet, affected) {
				if (err) {
					onFail(err);
					return;
				}
				if (affected) {
					if (!--count) onSuccess();
					return;
				}
				var request = new sql.Request(connection);
				if (record.path) request.input('path', sql.NVarChar(4000), record.path);
				forEach(record.params, function (value, name) {
					request.input(name, varMap[name], value);
				});
				query = 'INSERT INTO ' + tableName + ' (OwnerId, Path, ' +
					record.tokens.map(function (pair) { return pair[0]; }).join(', ') + ') VALUES (' +
					'\'' + record.ownerId + '\', ' + (record.path ? '@path' : 'NULL') + ', ' +
					record.tokens.map(function (pair) { return pair[1]; }).join(', ') + ')';
				request.query(query, function (err) {
					if (err) {
						onFail(err);
						return;
					}
					if (!--count) onSuccess();
				});
			});
		});
	});
	return def.promise;
}, 1);
