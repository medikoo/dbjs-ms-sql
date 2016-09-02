'use strict';

var assign            = require('es5-ext/object/assign')
  , forEach           = require('es5-ext/object/for-each')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , d                 = require('d')
  , lazy              = require('d/lazy')
  , deferred          = require('deferred')
  , ReducedStorage    = require('dbjs-persistence/reduced-storage')
  , sql               = require('mssql')
  , addToBulk         = require('./lib/add-to-bulk')
  , createTable       = require('./lib/create-table')
  , resolveRecord     = require('./lib/resolve-record')
  , handleTransaction = require('./lib/handle-transaction')

  , nextTick = process.nextTick;

var MsSqlReducedStorage = module.exports = function (driver) {
	if (!(this instanceof MsSqlReducedStorage)) return new MsSqlReducedStorage(driver);
	ReducedStorage.call(this, driver);
	this.connection = this.driver.connection(function (connection) {
		return createTable(connection, '_Reduced')(connection);
	});
};
setPrototypeOf(MsSqlReducedStorage, ReducedStorage);

MsSqlReducedStorage.prototype = Object.create(ReducedStorage.prototype, assign({
	constructor: d(MsSqlReducedStorage),

	// Any data
	__get: d(function (ns, path) {
		var query = 'SELECT * FROM [_Reduced]', params = {}, id;
		params.path = path;
		query += ' WHERE ((OwnerId = \'' + ns + '\') AND ';
		if (path) {
			query += '(Path = @path)';
			id = ns + '/' + path;
		} else {
			query += '(Path IS NULL)';
			id = ns;
		}
		query += ')';
		return this._query_(query, params)(function (result) { return result[id] || null; });
	}),
	__store: d(function (ns, path, data) { return this._store_(ns, path, data); }),
	__getObject: d(function (ownerId, keyPaths) {
		var query = 'SELECT * FROM [_Reduced] WHERE OwnerId = \'' + ownerId + '\'';
		var keyPathQueries = [], params = {}, index = 0;
		if (keyPaths) {
			keyPaths.forEach(function (keyPath) {
				++index;
				var path = 'path' + index, pathLike = 'pathLike' + index;
				params[path] = keyPath;
				params[pathLike] = keyPath + '*%';
				keyPathQueries.push('Path = @' + path);
				keyPathQueries.push('Path LIKE @' + pathLike);
			});
			query += ' AND ((Path IS NULL) OR (' + keyPathQueries.join(') OR (') + '))';
		}
		return this._query_(query, params);
	}),

	// Storage import/export
	__exportAll: d(function (destStorage) {
		return this._exportTable_(destStorage);
	}),
	__clear: d(function () {
		return this.connection(function (connection) {
			var request = new sql.Request(connection);
			return request.queryPromised('DELETE FROM [_Reduced]');
		}.bind(this));
	}),
	__drop: d(function () {
		return this.connection(function (connection) {
			var request = new sql.Request(connection);
			return request.queryPromised('DROP TABLE [_Reduced]');
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return deferred(null); }),

	// Driver specific
	_query_: d(function (query, params) {
		return this.connection(function (connection) {
			var request = new sql.Request(connection), def = deferred(), result = Object.create(null);
			if (params) {
				forEach(params, function (value, name) { request.input(name, sql.NVarChar(4000), value); });
			}
			request.stream = true;
			request.query(query + ' ORDER BY Stamp');
			request.on('error', def.reject);
			request.on('done', function () { def.resolve(result); });
			request.on('row', function (row) {
				row = resolveRecord(row);
				result[row.id] = row;
			});
			return def.promise;
		});
	}),
	_exportTable_: d(function (destStorage) {
		var promise = this.connection(function (connection) {
			var query = 'SELECT * FROM [_Reduced]';
			var def = deferred(), count = 0, promises = [];
			var request = new sql.Request(connection);
			request.stream = true;
			request.query(query);
			request.on('error', def.reject);
			request.on('row', function (row) {
				if (!(++count % 1000)) promise.emit('progress');
				var data = resolveRecord(row);
				promises.push(destStorage.__store(data.ownerId, data.path, data));
			});
			request.on('done', function () { def.resolve(deferred.map(promises)); });
			return def.promise;
		}.bind(this));
		return promise;
	}),
	_store_: d(function (ownerId, path, data) {
		return this._dbBatch_(function (rows) {
			addToBulk(rows, false, ownerId, path, data);
		})(this._dbBatchDeferred_.promise);
	})
}, lazy({
	_dbBatchDeferred_: d(function () { return deferred(); }),
	_dbBatch_: d(function () {
		return this.connection(function (connection) {
			var records = [];
			nextTick(function () {
				var def = this._dbBatchDeferred_;
				delete this._dbBatch_;
				delete this._dbBatchDeferred_;
				handleTransaction(connection, '_Reduced', def, records);
			}.bind(this));
			return records;
		}.bind(this));
	})
})));
