'use strict';

var setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , uncapitalize   = require('es5-ext/string/#/uncapitalize')
  , d              = require('d')
  , deferred       = require('deferred')
  , Driver         = require('dbjs-persistence/driver')
  , sql            = require('mssql')
  , Storage        = require('./storage')
  , ReducedStorage = require('./reduced-storage')

  , isIdent = RegExp.prototype.test.bind(/^[A-Z][a-z0-9A-Z]*$/)

  , promisify = deferred.promisify;

Object.defineProperties(sql.Connection.prototype, {
	connectPromised: d(promisify(sql.Connection.prototype.connect)),
	closePromised: d(promisify(sql.Connection.prototype.close))
});
Object.defineProperties(sql.Request.prototype, {
	queryPromised: d(promisify(sql.Request.prototype.query))
});

var MsSqlDriver = module.exports = Object.defineProperties(function (data) {
	var connectionData;
	if (!(this instanceof MsSqlDriver)) return new MsSqlDriver(data);
	ensureObject(data);
	ensureObject(data.msSql);
	connectionData = {};
	connectionData.user = ensureString(data.msSql.user);
	connectionData.password = ensureString(data.msSql.password);
	connectionData.database = ensureString(data.msSql.database);
	connectionData.server = data.msSql.server ? ensureString(data.msSql.server) : 'localhost';
	var connection = (new sql.Connection(connectionData));
	this.connection = connection.connectPromised()(connection).aside(null, this.emitError);
	Driver.call(this, data);
}, {
	storageClass: d(Storage),
	reducedStorageClass: d(ReducedStorage)
});
setPrototypeOf(MsSqlDriver, Driver);

MsSqlDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(MsSqlDriver),

	__resolveAllStorages: d(function () {
		return this.connection(function (connection) {
			var request = new sql.Request(connection)
			  , query = request.queryPromised('SELECT TABLE_NAME FROM information_schema.tables');
			return query(function (data) {
				data[0].forEach(function (item) {
					var name = item.TABLE_NAME;
					if (!isIdent(name)) return;
					this.getStorage(uncapitalize.call(name));
				}, this);
			}.bind(this));
		}.bind(this));
	}),
	__close: d(function () {
		return this.connection(function (connection) {
			return connection.closePromised();
		});
	})
});
