'use strict';

module.exports = function (t, a) {
	a(typeof t('Foo').declare(), 'string');
};