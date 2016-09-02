'use strict';

var assign              = require('es5-ext/object/assign')
  , forEach             = require('es5-ext/object/for-each')
  , setPrototypeOf      = require('es5-ext/object/set-prototype-of')
  , capitalize          = require('es5-ext/string/#/capitalize')
  , d                   = require('d')
  , lazy                = require('d/lazy')
  , eePipe              = require('event-emitter/pipe')
  , deferred            = require('deferred')
  , Storage             = require('dbjs-persistence/storage')
  , resolveValue        = require('dbjs-persistence/lib/resolve-direct-value')
  , filterComputedValue = require('dbjs-persistence/lib/filter-computed-value')
  , sql                 = require('mssql')
  , addToBulk           = require('./lib/add-to-bulk')
  , createTable         = require('./lib/create-table')
  , resolveRecord       = require('./lib/resolve-record')
  , handleTransaction   = require('./lib/handle-transaction')

  , promisify = deferred.promisify
  , nextTick = process.nextTick;

Object.defineProperties(sql.Connection.prototype, {
	connectPromised: d(promisify(sql.Connection.prototype.connect)),
	closePromised: d(promisify(sql.Connection.prototype.close))
});
Object.defineProperties(sql.Request.prototype, {
	queryPromised: d(promisify(sql.Request.prototype.query))
});

var MsSqlStorage = module.exports = function (driver, name/*, options*/) {
	if (!(this instanceof MsSqlStorage)) return new MsSqlStorage(driver, name, arguments[2]);
	Storage.call(this, driver, name, arguments[2]);
	this.connection = this.driver.connection(function (connection) {
		return deferred(
			createTable(connection, this._tableName_),
			createTable(connection, this._tableName_ + '_Computed'),
			createTable(connection, this._tableName_ + '_Reduced')
		)(connection);
	}.bind(this));
};
setPrototypeOf(MsSqlStorage, Storage);

MsSqlStorage.prototype = Object.create(Storage.prototype, assign({
	constructor: d(MsSqlStorage),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		var query = 'SELECT * FROM [' + this._tableName_, params = {}, id;
		if (cat === 'reduced') query += '_Reduced]';
		else if (cat === 'direct') query += ']';
		if (cat === 'computed') {
			params.path = ns;
			query += '_Computed] WHERE ((OwnerId = \'' + path + '\') AND  (Path = @path))';
			id = path + '/' + ns;
		} else if (path) {
			params.path = path;
			query += ' WHERE ((OwnerId = \'' + ns + '\') AND (Path = @path))';
			id = ns + '/' + path;
		} else {
			query += ' WHERE ((OwnerId = \'' + ns + '\') AND (Path IS NULL))';
			id = ns;
		}
		return this._query_(query, params)(function (result) { return result[id] || null; });
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced_(ns, path, data);
		if (cat === 'computed') return this._storeComputed_(path, ns, data);
		return this._storeDirect_(ns, path, data);
	}),

	// Direct data
	__getObject: d(function (ownerId, objectPath, keyPaths) {
		var query = 'SELECT * FROM [' + this._tableName_ + '] WHERE OwnerId = \'' + ownerId + '\'';
		var keyPathQueries = [], params = {}, index = 0;
		if (keyPaths) {
			keyPaths.forEach(function (keyPath) {
				++index;
				var path = 'path' + index, pathLike = 'pathLike' + index;
				if (objectPath) keyPath = objectPath + '/' + keyPath;
				params[path] = keyPath;
				params[pathLike] = keyPath + '*%';
				keyPathQueries.push('Path = @' + path);
				keyPathQueries.push('Path LIKE @' + pathLike);
			});
			query += ' AND ((Path IS NULL) OR (' + keyPathQueries.join(') OR (') + '))';
		} else if (objectPath) {
			params.path = objectPath;
			params.pathLike1 = objectPath + '/%';
			params.pathLike2 = objectPath + '*%';
			query += ' AND ((Path IS NULL) OR (Path = @path) OR (Path LIKE @pathLike1) OR ' +
				'(Path LIKE @pathLike2))';
		}
		return this._query_(query, params);
	}),
	__getAllObjectIds: d(function () {
		return this._query_('SELECT * FROM [' + this._tableName_ + '] WHERE Path IS NULL');
	}),
	__getAll: d(function () { return this._query_('SELECT * FROM [' + this._tableName_ + ']'); }),

	// Reduced data
	__getReducedObject: d(function (ns, keyPaths) {
		var query = 'SELECT * FROM [' + this._tableName_ + '_Reduced] WHERE OwnerId = \'' + ns + '\'';
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
			query += ' AND ((Path IS NULL) IR (' + keyPathQueries.join(') OR (') + '))';
		}
		return this._query_(query, params);
	}),

	// Storage import/export
	__exportAll: d(function (destStorage) {
		var promise;
		var promises = [
			this._exportTable_('direct', destStorage),
			this._exportTable_('computed', destStorage),
			this._exportTable_('computed', destStorage)
		];
		promise = deferred.map(promises);
		promises.forEach(function (tablePromise) { eePipe(tablePromise, promise); });
		return promise;
	}),
	__clear: d(function () {
		return this.connection(function (connection) {
			var directRequest = new sql.Request(connection)
			  , computedRequest = new sql.Request(connection)
			  , reducedRequest = new sql.Request(connection);
			return deferred.map(
				directRequest.queryPromised('DELETE FROM [' + this._tableName_ + ']'),
				computedRequest.queryPromised('DELETE FROM [' + this._tableName_ + '_Computed]'),
				reducedRequest.queryPromised('DELETE FROM [' + this._tableName_ + '_Reduced]')
			);
		}.bind(this));
	}),
	__drop: d(function () {
		return this.connection(function (connection) {
			var directRequest = new sql.Request(connection)
			  , computedRequest = new sql.Request(connection)
			  , reducedRequest = new sql.Request(connection);
			return deferred.map(
				directRequest.queryPromised('DROP TABLE [' + this._tableName_ + ']'),
				computedRequest.queryPromised('DROP TABLE [' + this._tableName_ + '_Computed]'),
				reducedRequest.queryPromised('DROP TABLE [' + this._tableName_ + '_Reduced]')
			);
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return deferred(null); }),

	__search: d(function (keyPath, value, callback) {
		return this.connection(function (connection) {
			var def = deferred()
			  , request = new sql.Request(connection)
			  , query = 'SELECT * FROM [' + this._tableName_ + ']';
			request.stream = true;
			if (keyPath !== undefined) {
				query += ' WHERE ';
				if (keyPath) {
					request.input('path', sql.NText, keyPath);
					request.input('pathLike', sql.NText, keyPath + '*%');
					query += '(Path = @path) OR (Path LIKE @pathLike)';
				} else {
					query += 'Path IS NULL';
				}
			}
			request.query(query);
			request.on('error', function (err) {
				if (err.code === 'ECANCEL') def.resolve();
				else def.reject(err);
			});
			request.on('done', def.resolve);
			request.on('row', function (row) {
				var data = resolveRecord(row), resolvedValue;
				if (value != null) {
					resolvedValue = resolveValue(data.ownerId, data.path, data.value);
					if (value !== resolvedValue) return;
				}
				if (callback(data.id, data)) request.cancel();
			});
			return def.promise;
		}.bind(this));
	}),
	__searchComputed: d(function (keyPath, value, callback) {
		return this.connection(function (connection) {
			var def = deferred()
			  , request = new sql.Request(connection)
			  , query = 'SELECT * FROM [' + this._tableName_ + '_Computed]';
			request.stream = true;
			if (keyPath) {
				request.input('path', sql.NText, keyPath);
				query += ' WHERE Path = @path';
			}
			request.query(query);
			request.on('error', function (err) {
				if (err.code === 'ECANCEL') def.resolve();
				else def.reject(err);
			});
			request.on('done', def.resolve);
			request.on('row', function (row) {
				var data = resolveRecord(row);
				if ((value != null) && !filterComputedValue(value, data.value)) return;
				if (callback(data.id, data)) request.cancel();
			});
			return def.promise;
		}.bind(this));
	}),

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
	_exportTable_: d(function (kind, destStorage) {
		var promise = this.connection(function (connection) {
			var query = 'SELECT * FROM [' + this._tableName_;
			if (kind === 'computed') query += '_Computed]';
			else if (kind === 'reduced') query += '_Reduced]';
			else query += ']';
			var def = deferred(), count = 0, promises = [];
			var request = new sql.Request(connection);
			request.stream = true;
			request.query(query);
			request.on('error', def.reject);
			request.on('row', function (row) {
				if (!(++count % 1000)) promise.emit('progress');
				var data = resolveRecord(row);
				if (kind === 'computed') {
					promises.push(destStorage.__storeRaw(kind, data.path, data.ownerId, data));
				} else {
					promises.push(destStorage.__storeRaw(kind, data.ownerId, data.path, data));
				}
			});
			request.on('done', function () { def.resolve(deferred.map(promises)); });
			return def.promise;
		}.bind(this));
		return promise;
	}),
	_storeDirect_: d(function (ownerId, path, data) {
		return this._directDbBatch_(function (rows) {
			addToBulk(rows, false, ownerId, path, data);
		})(this._directDbBatchDeferred_.promise);
	}),
	_storeComputed_: d(function (ownerId, keyPath, data) {
		return this._computedDbBatch_(function (rows) {
			addToBulk(rows, true, ownerId, keyPath, data);
		})(this._computedDbBatchDeferred_.promise);
	}),
	_storeReduced_: d(function (ownerId, keyPath, data) {
		return this._reducedDbBatch_(function (rows) {
			addToBulk(rows, false, ownerId, keyPath, data);
		})(this._reducedDbBatchDeferred_.promise);
	})
}, lazy({
	_tableName_: d(function () { return capitalize.call(this.name); }),

	_directDbBatchDeferred_: d(function () { return deferred(); }),
	_directDbBatch_: d(function () {
		return this.connection(function (connection) {
			var records = [];
			nextTick(function () {
				var def = this._directDbBatchDeferred_;
				delete this._directDbBatch_;
				delete this._directDbBatchDeferred_;
				handleTransaction(connection, this._tableName_, def, records);
			}.bind(this));
			return records;
		}.bind(this));
	}),

	_computedDbBatchDeferred_: d(function () { return deferred(); }),
	_computedDbBatch_: d(function () {
		return this.connection(function (connection) {
			var records = [];
			nextTick(function () {
				var def = this._computedDbBatchDeferred_;
				delete this._computedDbBatch_;
				delete this._computedDbBatchDeferred_;
				handleTransaction(connection, this._tableName_ + '_Computed', def, records);
			}.bind(this));
			return records;
		}.bind(this));
	}),
	_reducedDbBatchDeferred_: d(function () { return deferred(); }),
	_reducedDbBatch_: d(function () {
		return this.connection(function (connection) {
			var records = [];
			nextTick(function () {
				var def = this._reducedDbBatchDeferred_;
				delete this._reducedDbBatch_;
				delete this._reducedDbBatchDeferred_;
				handleTransaction(connection, this._tableName_ + '_Reduced', def, records);
			}.bind(this));
			return records;
		}.bind(this));
	})
})));
