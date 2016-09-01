'use strict';

var getTests          = require('dbjs-persistence/test/_common')
  , storageSplitTests = require('dbjs-persistence/test/_storage-split')

  , env, copyEnv, splitEnv, tests;

try {
	env = require('../env');
} catch (e) {
	env = null;
}

if (env) {
	copyEnv = Object.create(env);
	copyEnv.database = copyEnv.database + '-COPY';

	splitEnv = Object.create(env);
	splitEnv.database = splitEnv.database + '-SPLIT';

	tests = getTests({ msSql: env }, { msSql: copyEnv });
}

module.exports = function (t, a, d) {
	if (!env) {
		console.error("No database configuration (env.json), unable to proceed with test");
		d();
		return;
	}
	tests.apply(null, arguments)(function () {
		return storageSplitTests(t, { msSql: splitEnv }, a);
	}).done(function () { d(); }, d);
};
