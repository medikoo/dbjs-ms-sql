'use strict';

module.exports = function (t, a) {
	var rows = [];
	t(rows, false, 'aaa', 'bbb', { stamp: 123, value: '3234' });
	a.deep(rows, [{
		ownerId: 'aaa',
		path: 'bbb',
		params: { string: '234' },
		tokens: [
			['Stamp', '123'],
			['ValueType', '3'],
			['ValueBoolean', 'NULL'],
			['ValueNumber', 'NULL'],
			['ValueString', '@string'],
			['ValueDateTime', 'NULL'],
			['ValueRegExp', 'NULL'],
			['ValueFunction', 'NULL'],
			['ValueObject', 'NULL']
		]
	}]);
	rows = [];
	t(rows, true, 'aaa', 'bbb', { stamp: 123, value: '2234' });
	a.deep(rows, [{
		ownerId: 'aaa',
		path: 'bbb',
		params: {},
		tokens: [
			['Stamp', '123'],
			['ValueType', '2'],
			['ValueBoolean', 'NULL'],
			['ValueNumber', '234'],
			['ValueString', 'NULL'],
			['ValueDateTime', 'NULL'],
			['ValueRegExp', 'NULL'],
			['ValueFunction', 'NULL'],
			['ValueObject', 'NULL'],
			['ValueMultiple', 'NULL']
		]
	}]);
};
